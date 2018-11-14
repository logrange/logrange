// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sync

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/logrange/logrange/pkg/util"
)

type (
	// A RWLock is a reader/writer mutual exclusion lock.
	// The lock can be held by an arbitrary number of readers or a single writer.
	// The zero value for a RWLock is an unlocked object.
	//
	// A RWLock must not be copied after first use.
	//
	// The behavior of the RWLock is same as sync.RWMutex, but the RWLock could
	// be Closed. The closing of the RWLock means that any attempt to lock it
	// by RLock() or Lock() functions will get an error result. All blocked
	// go routines in the calls RLock() or Lock() will be unblocked with the
	// error result as well.
	RWLock struct {
		readers int32
		state   int
		writers int32
		lock    sync.Mutex
		clsCh   chan struct{}
		rrCh    chan bool
		wrCh    chan bool
	}
)

const rwLockMaxReaders = 1 << 30

const (
	// stateNew indicates that the RWLock is not initialized yet
	stateNew = 0

	// stateInit shows that writer is unlocked, if readers == 0, then
	// the RWLock is not locked
	stateInit = 1

	// stateWaiting indicates that there are write-locks requests, that waiting
	// for the lock for writing.
	stateWaiting = 2

	// stateNotifying one of unlockers (either read or write) is going to send
	// a notification event
	stateNotifying = 3

	// stateLocked shows that RWLock is locked for write
	stateLocked = 4

	// the RWLock is closed and cannot be used anymore
	stateClosed = 5
)

// Close causes the RWLock is closed and cannot be used anymore. Locking
// functions RLock() and Lock() must return an error for a closed RWLock.
func (rw *RWLock) Close() error {
	rw.lock.Lock()
	defer rw.lock.Unlock()

	if rw.state == stateClosed {
		return util.ErrWrongState
	}

	// to guarantee that RLock does check the state
	atomic.AddInt32(&rw.readers, -rwLockMaxReaders)

	rw.state = stateClosed
	if rw.clsCh != nil {
		close(rw.clsCh)
	}
	return nil
}

// RLock locks rw for reading. The read lock is reentrant, so may be used
// multiple readers from many go-routines or from a one as well.
// If the lock is already locked for writing,
// RLock blocks until the lock is available or closed.
func (rw *RWLock) RLock() error {
	if atomic.AddInt32(&rw.readers, 1) > 0 {
		return nil
	}

	var ch chan bool
	rw.lock.Lock()
	if !rw.init() {
		rw.lock.Unlock()
		atomic.AddInt32(&rw.readers, -1)
		return util.ErrWrongState
	}

	if rw.state == stateInit || rw.state == stateWaiting {
		// ok, the state is ok for readers acquisition
		rw.lock.Unlock()
		return nil
	}

	// will wait when will be allowed for a read
	ch = rw.rrCh
	rw.lock.Unlock()

	select {
	case <-rw.clsCh:
		atomic.AddInt32(&rw.readers, -1)
		return util.ErrWrongState
	case <-ch:
		// got it
	}
	return nil
}

// RUnlock undoes a single RLock call;
// it does not affect other simultaneous readers.
func (rw *RWLock) RUnlock() {
	r := atomic.AddInt32(&rw.readers, -1) + rwLockMaxReaders
	// no writers?
	if r > 0 {
		return
	}

	if r < 0 {
		panic(fmt.Sprintf("Incorrect (negative) readers counter rc=%s", rw))
	}

	rw.lock.Lock()
	if rw.state == stateLocked {
		rw.lock.Unlock()
		panic(fmt.Sprintf("Unexpected state in releaseRead %s", rw))
	}

	if rw.state != stateWaiting {
		// seems like no writers here, no need to notify
		rw.state = stateInit
		rw.lock.Unlock()
		return
	}
	rw.state = stateNotifying
	rw.lock.Unlock()

	// notify a writer, or closed
	rw.notifyWriter()
}

// Lock locks rw for writing.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available or closed.
func (rw *RWLock) Lock() error {
	return rw.LockWithCtx(context.Background())
}

// Lock locks rw for writing using context provided.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available, closed or provided context is closed.
func (rw *RWLock) LockWithCtx(ctx context.Context) error {
	rw.lock.Lock()
	if !rw.init() {
		rw.lock.Unlock()
		return util.ErrWrongState
	}

	rw.writers++
	if rw.writers == 1 {
		// first writer, creates rrCh notification channel
		rw.rrCh = make(chan bool)
		if atomic.AddInt32(&rw.readers, -rwLockMaxReaders) == -rwLockMaxReaders {
			// happy to lock
			rw.state = stateLocked
			rw.lock.Unlock()
			return nil
		}
	}
	if rw.state == stateInit {
		rw.state = stateWaiting
	}
	rw.lock.Unlock()

	select {
	case <-rw.clsCh:
		rw.cancelWriter()
		return util.ErrWrongState
	case <-ctx.Done():
		rw.cancelWriter()
		return ctx.Err()
	case <-rw.wrCh:
		rw.lock.Lock()
		rw.state = stateLocked
		rw.lock.Unlock()
	}

	return nil
}

func (rw *RWLock) cancelWriter() {
	rw.lock.Lock()
	rw.writers--
	if rw.writers == 0 {
		atomic.AddInt32(&rw.readers, rwLockMaxReaders)
		if rw.state == stateNotifying {
			<-rw.wrCh
			rw.state = stateInit
		} else if rw.state != stateClosed {
			rw.state = stateInit
		}
		close(rw.rrCh)
	}
	rw.lock.Unlock()
}

// Unlock unlocks rw for writing.
// if there are other goroutiens expecting the rw be locked for writing one of
// them will be able to lock. If there is no writers, then readers, if there are
// some, will be able to lock rw for reading.
func (rw *RWLock) Unlock() {
	rw.lock.Lock()

	if rw.writers == 0 {
		rw.lock.Unlock()
		panic(fmt.Sprintf("Incorrect writers counter=%d  or state rc=%s", rw.writers, rw))
	}
	rw.writers--

	if !rw.init() {
		rw.lock.Unlock()
		return
	}

	notifyWriter := rw.writers > 0
	if !notifyWriter {
		rw.state = stateInit
		atomic.AddInt32(&rw.readers, rwLockMaxReaders)
		// let readers know, if any, that there is no writers anymore
		close(rw.rrCh)
	} else {
		rw.state = stateNotifying
	}
	rw.lock.Unlock()

	if notifyWriter {
		rw.notifyWriter()
	}

}

func (rw *RWLock) notifyWriter() {
	select {
	case rw.wrCh <- true:
	case <-rw.clsCh:
	}
}

func (rw *RWLock) init() bool {
	if rw.state == stateClosed {
		return false
	}
	if rw.state != stateNew {
		return true
	}

	rw.state = stateInit
	rw.clsCh = make(chan struct{})
	rw.wrCh = make(chan bool)
	return true
}

func (rw *RWLock) String() string {
	return fmt.Sprintf("{rdrs=%d, wrtrs=%d, state=%d}", atomic.LoadInt32(&rw.readers), atomic.LoadInt32(&rw.writers), rw.state)
}
