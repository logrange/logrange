// Copyright 2018-2019 The logrange Authors
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

package chunkfs

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logrange/range/pkg/utils/errors"
)

type (
	// FdPool manages fReader(s) pool. It counts how many are created at
	// a moment and it doesn't allow to have more than a maximum value. FdPool caches
	// fReader(s) with a purpose to re-use oftenly used ones.
	FdPool struct {
		maxSize int32
		curSize int32
		lock    sync.Mutex
		sem     chan bool
		frs     map[uint64]*frPool
		closed  int32
		cchan   chan bool
	}

	// frParams struct contains params for creating a new fReader when requested
	frParams struct {
		fname            string
		bufSize          int
		createIfNotFound bool
	}

	// file readers pool
	frPool struct {
		fp   frParams
		rdrs []*fReader
	}
)

// NewFdPool creats new FdPool object with maxSize maximum fReader(s) capacity
func NewFdPool(maxSize int) *FdPool {
	if maxSize <= 0 {
		panic(fmt.Sprint("Expecting positive integer, but got maxSize=", maxSize))
	}

	fdp := new(FdPool)
	fdp.frs = make(map[uint64]*frPool, maxSize)
	fdp.sem = make(chan bool, maxSize)
	fdp.cchan = make(chan bool)
	fdp.freeSem(maxSize)
	fdp.maxSize = int32(maxSize)

	go func() {
		done := false
		for !done {
			select {
			case _, ok := <-fdp.cchan:
				if !ok {
					done = true
				}
			case <-time.After(time.Minute):
			}
			fdp.lock.Lock()
			fdp.clean(done)
			fdp.lock.Unlock()
		}
	}()
	return fdp
}

// register allows to register a group of fReader(s) by its group id (gid)
// clients will be able to acquire only registered groups (see acquire)
func (fdp *FdPool) register(gid uint64, fp frParams) error {
	fdp.lock.Lock()
	defer fdp.lock.Unlock()

	if _, ok := fdp.frs[gid]; ok {
		return fmt.Errorf("Oops the gid=%X is already registered here!", gid)
	}

	fdp.frs[gid] = newFRPool(fp)
	return nil
}

// acquire - allows to acquire fReader for the specified group (gid). It expects
// the gid and a desired offset, where the read operation will start from. It also
// receives a context in case of the pool reaches maximum capacity and the call
// will be blocking invoking go-routine until a fReader is released.
func (fdp *FdPool) acquire(ctx context.Context, gid uint64, offset int64) (*fReader, error) {
	fdp.lock.Lock()
	if atomic.LoadInt32(&fdp.closed) != 0 {
		fdp.lock.Unlock()
		return nil, errors.ClosedState
	}

	frp, ok := fdp.frs[gid]
	if !ok {
		fdp.lock.Unlock()
		return nil, errors.ClosedState
	}

	fr := frp.getFree(offset)
	if fr != nil {
		fdp.lock.Unlock()
		return fr, nil
	}

	if atomic.AddInt32(&fdp.curSize, 1) >= fdp.maxSize {
		fdp.clean(false)
	}

	fdp.lock.Unlock()

	select {
	case <-ctx.Done():
		atomic.AddInt32(&fdp.curSize, -1)
		return nil, ctx.Err()
	case _, ok := <-fdp.sem:
		// we have the ticket
		if !ok {
			atomic.AddInt32(&fdp.curSize, -1)
			return nil, errors.ClosedState
		}
		return fdp.createAndUseFreader(gid, frp.fp)
	}
}

// release - releases a fReader, which was acquired before
func (fdp *FdPool) release(fr *fReader) {
	if !fr.makeFree() {
		// ok, it was either closing or closed state or it was Free, what is wrong anyway
		fdp.lock.Lock()
		fr.close()
		fdp.lock.Unlock()
		return
	}

	if atomic.LoadInt32(&fdp.curSize) >= fdp.maxSize {
		fdp.lock.Lock()
		fdp.clean(false)
		fdp.lock.Unlock()
	}
}

func (fdp *FdPool) releaseAllByGid(gid uint64) {
	fdp.lock.Lock()
	defer fdp.lock.Unlock()

	frp, ok := fdp.frs[gid]
	if ok {
		cnt := frp.cleanUp(true)
		atomic.AddInt32(&fdp.curSize, -int32(cnt))
		fdp.freeSem(cnt)
		delete(fdp.frs, gid)
	}
}

// Close - closes the FdPool
func (fdp *FdPool) Close() error {
	if !atomic.CompareAndSwapInt32(&fdp.closed, 0, 1) {
		return errors.ClosedState
	}

	fdp.lock.Lock()
	defer fdp.lock.Unlock()

	fdp.clean(true)
	close(fdp.cchan)
	close(fdp.sem)
	return nil
}

func (fdp *FdPool) clean(all bool) {
	for nm, frp := range fdp.frs {
		cnt := frp.cleanUp(all)
		atomic.AddInt32(&fdp.curSize, -int32(cnt))
		fdp.freeSem(cnt)
		if all {
			delete(fdp.frs, nm)
		}
	}
}

func (fdp *FdPool) freeSem(cnt int) {
	if atomic.LoadInt32(&fdp.closed) != 0 {
		return
	}
	for i := 0; i < cnt; i++ {
		fdp.sem <- true
	}
}

func (fdp *FdPool) createAndUseFreader(gid uint64, fp frParams) (*fReader, error) {
	fr, err := newFReader(fp.fname, fp.bufSize)
	if os.IsNotExist(err) && fp.createIfNotFound {
		var f *os.File
		f, err = os.OpenFile(fp.fname, os.O_CREATE|os.O_RDWR, 0640)
		if err != nil {
			return nil, err
		}
		f.Close()

		fr, err = newFReader(fp.fname, fp.bufSize)
	}
	if err != nil {
		return nil, err
	}

	fdp.lock.Lock()
	frp, ok := fdp.frs[gid]
	if !ok {
		fdp.lock.Unlock()
		fr.Close()
		return nil, errors.ClosedState
	}

	frp.rdrs = append(frp.rdrs, fr)
	fr.makeBusy()
	fdp.lock.Unlock()
	return fr, nil
}

// ============================= frPool ======================================
func newFRPool(fp frParams) *frPool {
	frp := new(frPool)
	frp.fp = fp
	frp.rdrs = make([]*fReader, 0, 1)
	return frp
}

func (frp *frPool) getFree(offset int64) *fReader {
	ridx := -1
	var dist uint64 = math.MaxUint64
	for idx, fr := range frp.rdrs {
		if fr.isFree() {
			if ridx < 0 {
				ridx = idx
				dist = fr.distance(offset)
			} else {
				d := fr.distance(offset)
				if d < dist {
					ridx = idx
					dist = d
				}
			}
		}
	}

	if ridx < 0 {
		return nil
	}

	fr := frp.rdrs[ridx]
	fr.makeBusy()
	return fr
}

func (frp *frPool) cleanUp(all bool) int {
	cnt := 0
	for i := 0; i < len(frp.rdrs); i++ {
		fr := frp.rdrs[i]
		if !all && !fr.isFree() {
			continue
		}
		frp.rdrs[i] = frp.rdrs[len(frp.rdrs)-1]
		frp.rdrs[len(frp.rdrs)-1] = nil
		frp.rdrs = frp.rdrs[:len(frp.rdrs)-1]
		i--
		fr.Close()
		cnt++
	}
	return cnt
}

func (frp *frPool) isEmpty() bool {
	return len(frp.rdrs) == 0
}
