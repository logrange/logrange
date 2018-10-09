package jrnl

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type (
	FRPoolCtrlr struct {
		// buffered channel, used for global counting file-readers. Every time
		// when a file Reader is created, a value is added to the channel to be sure
		// it is counted
		rpChan chan bool
		// rpSize contains number of opened file Readers (rpChan size)
		rpSize int32

		// clsOnRlsThrshld indicates the buffer size, when file-readers cannot
		// be cached by FRPool anymore, but should be closed instantly after usage
		clsOnRlsThrshld int32
		lock            sync.Mutex
		frPools         map[string]*chnkFRPool // known pools
	}

	// per chunk
	frChunkPool struct {
		lock    *sync.Mutex
		pCtrlr  *FRPoolCtrlr
		rdrs    map[]*readersDesc
		rlsChan chan *fReader
		closed  bool
	}
)

func NewFRPool(maxSize, clsOnRlsThrshld int) (*FRPool, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("Max Pool Size must be positive, but %d is provided", maxSize)
	}

	if maxSize < clsOnRlsThrshld {
		return nil, fmt.Errorf("Max Pool Size=%d should be greater or equal to close on Release Threashold, which is %d", maxSize, clsOnRlsThrshld)
	}

	fp := new(FRPool)
	fp.reqPool = make(chan book, maxSize)
	fp.clsOnRlsThrshld = clsOnRlsThrshld
	fp.cfrPools = make(map[string]*chkFRdrPool)
}

func (fp *FRPool) Close() error {
	fp.lock.Lock()
	defer fp.lock.Unlock()

	if len(fp.frPools) > 0 {
		panic("Usage contract violation: FRPool must be closed only when all pools are closed")
	}
	cloase(fp.rpChan)
	fp.frPools = nil
}

// closeOnRelease is called by chnkFRPool when it tries to release a fReader.
// The function returns whether the fReader must be closed immediately, or can be
// cached for awhile
func (fp *FRPool) closeOnRelease() bool {
	return fp.clsOnRlsThrshld < atomic.LoadInt32(&fp.rpSize)
}

// onCloseFr lets the FRPool know about a fReader was closed
func (fp *FRPool) onCloseFr() {
	atomic.AddInt32(&fp.rpSize, -1)
	<-fp.rpChan
}

// ----------------------------- chnkFRPool ----------------------------------
// it must be called holding the lock
func (cfp *chnkFRPool) acquire() (*fReader, error) {
	fr := cfp.bestMatch()
	if fr != nil {
		return fr, nil
	}

	select {
	case cfp.globPool.rpChan <- true:
		// well, we could write into the channel
		return cfp.newFReader(), nil
	default:
	}

	cfp.lock.Unlock()
	select {
	case cfp.globPool.rpChan <- true:
		cfp.lock.Lock()
		if cfp.closed {
			// Oops, read it back
			<-cfp.globPool.rpChan
			return nil, fmt.Errorf("Already closed") // TODO: return an error
		}
		return cfp.newFReader(), nil
	case fr, rok := <-rlsChan:
		{
			cfp.lock.Lock()
			if cfp.closed {
				if rok {
					cfp.globPool.onCloseFr()
				}
				return nil, fmt.Errorf("Already closed") // TODO: return an error
			}

			return fr, nil
		}
	}
}

// called holding lock
func (cfp *chnkFRPool) release(fr *fReader) {
	if cfp.closed {
		cfp.closeFr(fr)
		return
	}

	select {
	case cfp.rlsChan <- fr:
		return
	default:
		// ok, nobody there
	}

	if cfp.globPool.closeOnRelease() {
		cfp.closeFr(fr)
		return
	}

	cfp.rdrs = append(cfp.rdrs, fr)
}

func (cfp *chnkFRPool) newFReader() *fReader {
	atomic.AddInt32(&cfp.globPool.rpSirpSize, 1)
	// TODO: implement the rest
}

func (cfp *chnkFRPool) closeFr(fr *fReader) {
	cfp.globPool.onCloseFr()
	fr.Close()
}

func (cfp *chnkFRPool) close() {
	for _, fr := range cfp.rdrs {
		cfp.closeFr(fr)
	}
	cfp.rdrs = nil
	close(cfp.rlsChan)
	cfp.closed = true
}

func (cfp *chnkFRPool) bestMatch() *fReader {
	// find the best reader match or return nil
}

// ---------------------------- frChunkPool ----------------------------------
