package fs

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// FdPool struct manages fReader(s) pool. It counts how many are created at
	// a moment and doesn't allow to have more than a maximum value. FdPool caches
	// fReader(s) with a purpose to re-use oftenly used ones.
	FdPool struct {
		maxSize int32
		curSize int32
		lock    sync.Mutex
		sem     chan bool
		frs     map[string]*frPool
		closed  int32
		cchan   chan bool
	}

	// file readers pool
	frPool struct {
		rdrs []*fReader
	}
)

const (
	cFrsInUse = 1
	cFrsFree  = 0

	cFrBufSize = 32 * 1024
)

// NewFdPool creats new FdPool object with maxSize maximum fReader(s) capacity
func NewFdPool(maxSize int) *FdPool {
	if maxSize <= 0 {
		panic(fmt.Sprint("Expecting positive integer, but got maxSize=", maxSize))
	}

	fdp := new(FdPool)
	fdp.frs = make(map[string]*frPool)
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

// Acquire - allows to acquire fReader for the specified name. It expects the file
// name and a desired offset, where the read operation will start from. It also
// receives a context in case of the pool reaches maximum capacity and the call
// will be blocking invoking go-routine until a fReader is released.
func (fdp *FdPool) Acquire(ctx context.Context, name string, offset int64) (*fReader, error) {
	fdp.lock.Lock()
	if atomic.LoadInt32(&fdp.closed) != 0 {
		fdp.lock.Unlock()
		return nil, ErrWrongState
	}

	if frp, ok := fdp.frs[name]; ok {
		fr := frp.getFree(offset)
		if fr != nil {
			fdp.lock.Unlock()
			return fr, nil
		}
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
			return nil, ErrWrongState
		}
		return fdp.createAndUseFreader(name)
	}
}

// Release - releases a fReader, which was acquired before
func (fdp *FdPool) Release(fr *fReader) {
	atomic.StoreInt32(&fr.plState, cFrsFree)
	if atomic.LoadInt32(&fdp.curSize) >= fdp.maxSize {
		fdp.lock.Lock()
		fdp.clean(false)
		fdp.lock.Unlock()
	}

	if atomic.LoadInt32(&fdp.closed) != 0 {
		fr.Close()
	}
}

// Close - closes the FdPool
func (fdp *FdPool) Close() error {
	if atomic.LoadInt32(&fdp.closed) != 0 {
		return ErrWrongState
	}

	fdp.lock.Lock()
	defer fdp.lock.Unlock()

	atomic.StoreInt32(&fdp.closed, 1)
	close(fdp.cchan)
	close(fdp.sem)
	return nil
}

func (fdp *FdPool) clean(all bool) {
	for nm, frp := range fdp.frs {
		cnt := frp.cleanUp(all)
		fdp.curSize -= int32(cnt)
		fdp.freeSem(cnt)
		if frp.isEmpty() {
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

func (fdp *FdPool) createAndUseFreader(name string) (*fReader, error) {
	fr := newFReader(name, cFrBufSize)
	err := fr.open()
	if err != nil {
		return nil, err
	}
	fr.plState = cFrsInUse

	fdp.lock.Lock()
	frp, ok := fdp.frs[name]
	if !ok {
		frp = newFRPool()
		fdp.frs[name] = frp
	}

	frp.rdrs = append(frp.rdrs, fr)
	fdp.lock.Unlock()
	return fr, nil
}

// ============================= frPool ======================================
func newFRPool() *frPool {
	frp := new(frPool)
	frp.rdrs = make([]*fReader, 0, 1)
	return frp
}

func (frp *frPool) getFree(offset int64) *fReader {
	ridx := -1
	var dist uint64 = math.MaxUint64
	for idx, fr := range frp.rdrs {
		if atomic.LoadInt32(&fr.plState) == cFrsFree {
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
	atomic.StoreInt32(&fr.plState, cFrsInUse)
	return fr
}

func (frp *frPool) cleanUp(all bool) int {
	cnt := 0
	for i := 0; i < len(frp.rdrs); i++ {
		fr := frp.rdrs[i]
		if !all && atomic.LoadInt32(&fr.plState) == cFrsInUse {
			continue
		}
		frp.rdrs[i] = frp.rdrs[len(frp.rdrs)-1]
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
