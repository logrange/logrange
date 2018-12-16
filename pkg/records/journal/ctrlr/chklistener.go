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

package ctrlr

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/records/journal"
	"github.com/logrange/logrange/pkg/util"
)

type (
	chunkListener struct {
		lock sync.Mutex
		cond *sync.Cond
		cc   *chnksController

		// data waiters, the goroutines which waits till a new data appears
		waiters int32
		dwChnls []chan bool

		clsdCh chan struct{}
		clsd   bool
	}
)

func newChunkListener(cc *chnksController) *chunkListener {
	ccl := new(chunkListener)
	ccl.cond = sync.NewCond(&ccl.lock)
	return ccl
}

func (cl *chunkListener) close() {
	cl.lock.Lock()
	if cl.clsd {
		cl.lock.Unlock()
		return
	}
	cl.clsd = true
	cl.lock.Unlock()
	close(cl.clsdCh)
}

// OnNewData is a part of chunk.Listener
func (cl *chunkListener) OnNewData(c chunk.Chunk) {
	if atomic.LoadInt32(&cl.waiters) <= 0 {
		return
	}

	cl.lock.Lock()
	for i, ch := range cl.dwChnls {
		close(ch)
		cl.dwChnls[i] = nil
	}

	if cap(cl.dwChnls) > 10 {
		cl.dwChnls = nil
	} else {
		cl.dwChnls = cl.dwChnls[:0]
	}

	cl.lock.Unlock()
}

// waitData allows to wait a new data is written into the journal. The function
// expects ctx context and curId a record Id to check the operation against to.
// The call will block current go-routine, if the curId lies behind the last
// record in the journal. The case, the go-routine will be unblock until one of the
// two events happen - ctx is closed or new records added to the journal.
func (cl *chunkListener) waitData(ctx context.Context, curId journal.Pos) error {
	atomic.AddInt32(&cl.waiters, 1)
	defer atomic.AddInt32(&cl.waiters, -1)

	for {
		cl.lock.Lock()
		if cl.clsd {
			cl.lock.Unlock()
			return util.ErrWrongState
		}

		chk, err := cl.cc.getLastChunk(ctx)
		if err != nil {
			cl.lock.Unlock()
			return err
		}

		if chk != nil {
			lro := journal.Pos{chk.Id(), chk.Count()}
			if curId.Less(lro) {
				cl.lock.Unlock()
				return nil
			}
			curId = lro
		}

		ch := make(chan bool)
		if cl.dwChnls == nil {
			cl.dwChnls = make([]chan bool, 0, 10)
		}
		cl.dwChnls = append(cl.dwChnls, ch)
		cl.lock.Unlock()

		select {
		case <-ctx.Done():
			cl.lock.Lock()
			ln := len(cl.dwChnls)
			for i, c := range cl.dwChnls {
				if c == ch {
					cl.dwChnls[i] = cl.dwChnls[ln-1]
					cl.dwChnls[ln-1] = nil
					cl.dwChnls = cl.dwChnls[:ln-1]
					close(ch)
					break
				}
			}
			cl.lock.Unlock()
			return ctx.Err()
		case <-ch:
			// the select
		case <-cl.clsdCh:
			return util.ErrWrongState
		}
	}
}
