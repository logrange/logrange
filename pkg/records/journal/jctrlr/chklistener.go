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

package jctrlr

import (
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	chunkListener struct {
		lock sync.Mutex
		cond *sync.Cond

		// data waiters, the goroutines which waits till a new data appears
		waiters int32
		dwChnls []chan bool
	}
)

// -------------------------- chunk.Listener ---------------------------------
func (cl *chunkListener) OnNewData(c chunk.Chunk) {
	if atomic.LoadInt32(&j.waiters) <= 0 {
		return
	}

	cl.lock.Lock()
	defer cl.lock.Unlock()

	for i, ch := range cl.dwChnls {
		close(ch)
		cl.dwChnls[i] = nil
	}

	if cap(cl.dwChnls) > 10 {
		cl.dwChnls = nil
	} else {
		cl.dwChnls = cl.dwChnls[:0]
	}
}

// waitData allows to wait a new data is written into the journal. The function
// expects ctx context and curId a record Id to check the operation against to.
// The call will block current go routine, if the curId lies behind the last
// record in the journal. The case, the goroutine will be unblock until one of the
// two events happen - ctx is closed or new records added to the journal.
func (cl *chunkListener) waitData(ctx context.Context, curId Pos) error {
	atomic.AddInt32(&j.waiters, 1)
	defer atomic.AddInt32(&j.waiters, -1)

	for {
		j.lock.Lock()
		chk := j.getLastChunk()
		lro := Pos{chk.Id(), chk.Count()}
		if curId.Less(lro) {
			j.lock.Unlock()
			return nil
		}
		curId = lro

		ch := make(chan bool)
		if j.dwChnls == nil {
			j.dwChnls = make([]chan bool, 0, 10)
		}
		j.dwChnls = append(j.dwChnls, ch)
		j.lock.Unlock()

		select {
		case <-ctx.Done():
			j.lock.Lock()
			ln := len(j.dwChnls)
			for i, c := range j.dwChnls {
				if c == ch {
					j.dwChnls[i] = j.dwChnls[ln-1]
					j.dwChnls[ln-1] = nil
					j.dwChnls = j.dwChnls[:ln-1]
					close(ch)
					break
				}
			}
			j.lock.Unlock()
			return ctx.Err()
		case <-ch:
			// the select
		}
	}
}
