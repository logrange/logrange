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
		sync.RWMutex
		readers int32
		wLock   bool
		writers int32
		lock    sync.Mutex
		clsCh   chan struct{}
		rrCh    chan bool
		wrCh    chan bool
		closed  bool
	}
)

const rwLockMaxReaders = 1 << 30

// Close causes the RWLock is closed and cannot be used anymore. Locking
// functions RLock() and Lock() must return an error for a closed RWLock.
func (rw *RWLock) Close() error {
	rw.lock.Lock()
	defer rw.lock.Unlock()

	if rw.closed {
		return util.ErrWrongState
	}

	atomic.AddInt32(&rw.readers, -rwLockMaxReaders)

	rw.closed = true
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

	if !rw.wLock {
		// ok, readers are fine still
		rw.lock.Unlock()
		return nil
	}

	// will wait writers releasing
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
	r := atomic.AddInt32(&rw.readers, -1)
	// no writers?
	if r >= 0 {
		return
	}

	if r < -rwLockMaxReaders {
		panic(fmt.Sprintf("Incorrect (negative) readers counter rc=%s", rw))
	}

	rw.lock.Lock()
	if rw.wLock || rw.writers == 0 {
		rw.lock.Unlock()
		panic(fmt.Sprintf("Unexpected state in releaseRead %s", rw))
	}

	if rw.writers <= 0 {
		// seems like writers gone?
		rw.lock.Unlock()
		return
	}

	if !rw.init() {
		rw.lock.Unlock()
		return
	}
	rw.lock.Unlock()

	// notify a writer, or closed
	rw.notifyWriter()
}

// Lock locks rw for writing.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available or closed.
func (rw *RWLock) Lock() error {
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
			rw.wLock = true
			rw.lock.Unlock()
			return nil
		}
	}
	rw.lock.Unlock()

	select {
	case <-rw.clsCh:
		rw.lock.Lock()
		rw.writers--
		if rw.writers == 0 {
			atomic.AddInt32(&rw.readers, rwLockMaxReaders)
			close(rw.rrCh)
		}
		rw.lock.Unlock()
		return util.ErrWrongState
	case <-rw.wrCh:
		rw.lock.Lock()
		rw.wLock = true
		rw.lock.Unlock()
	}

	return nil
}

// Unlock unlocks rw for writing.
// if there are other goroutiens expecting the rw be locked for writing one of
// them will be able to lock. If there is no writers, then readers, if there are
// some, will be able to lock rw for reading.
func (rw *RWLock) Unlock() {
	rw.lock.Lock()

	if rw.writers == 0 || !rw.wLock {
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
		rw.wLock = false
		atomic.AddInt32(&rw.readers, rwLockMaxReaders)
		// let readers know, if any, that there is no writers anymore
		close(rw.rrCh)
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
	if rw.closed {
		return false
	}
	if rw.clsCh != nil {
		return true
	}

	rw.clsCh = make(chan struct{})
	rw.wrCh = make(chan bool)
	return true
}

func (rw *RWLock) String() string {
	return fmt.Sprintf("{rdrs=%d, wrtrs=%d, wLock=%t, closed=%t}", atomic.LoadInt32(&rw.readers), atomic.LoadInt32(&rw.writers), rw.wLock, rw.closed)
}
