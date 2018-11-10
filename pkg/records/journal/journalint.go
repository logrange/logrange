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

package journal

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/util"
)

type (
	journal struct {
		name   string
		cc     ChnksController
		logger log4g.Logger
		lock   sync.Mutex
		cond   *sync.Cond

		// data waiters, the goroutines which waits till a new data appears
		waiters int32
		dwChnls []chan bool

		// wsem is for creating new chunks while write
		wsem chan bool

		// closed the journal state flag
		closed bool

		// contains []records.Chunk in sorted order
		chunks atomic.Value

		// settings
		maxChunkSize uint64
	}
)

func New(cc ChnksController, jname string) Journal {
	j := new(journal)
	j.name = jname
	j.cc = cc
	j.cond = sync.NewCond(&j.lock)
	j.chunks.Store(cc.GetChunks(j))

	// prepare wsem. one element is there
	j.wsem = make(chan bool, 1)
	j.wsem <- true
	j.logger = log4g.GetLogger("journal").WithId("{" + j.name + "}").(log4g.Logger)
	j.logger.Info("New instance created ", j)
	return j
}

// Name returns the name of the journal
func (j *journal) Name() string {
	return j.name
}

// Write - writes records received from the iterator to the journal.
func (j *journal) Write(ctx context.Context, rit records.Iterator) (int, Pos, error) {
	var err error
	var c chunk.Chunk
	for _, err = rit.Get(ctx); err == nil; {
		c, err = j.getWriterChunk(ctx, c)
		if err != nil {
			return 0, Pos{}, err
		}

		// we either would be able to write something or all to the chunk, or will get
		// an error
		n, offs, err := c.Write(ctx, rit)
		if n > 0 {
			return n, Pos{c.Id(), offs}, err
		}

		if err == util.ErrMaxSizeReached {
			continue
		}
		break
	}
	return 0, Pos{}, err
}

func (j *journal) Iterator() (Iterator, error) {
	return nil, nil
}

func (j *journal) Size() int64 {
	chunks := j.chunks.Load().(chunk.Chunks)
	var sz int64
	for _, c := range chunks {
		sz += c.Size()
	}
	return sz
}

func (j *journal) Close() error {
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.closed {
		j.logger.Warn("Journal is already closed, ignoring this Close() call ", j)
		return nil
	}

	j.logger.Info("Closing the journal ", j)
	j.closed = true

	close(j.wsem)
	j.chunks.Store(make(chunk.Chunks, 0, 0))

	return nil
}

func (j *journal) String() string {
	chnks := j.chunks.Load().(chunk.Chunks)
	return fmt.Sprintf("{name=%s, chunks=%d, closed=%t}", j.name, len(chnks), j.closed)
}

// --------------------------- ChunkListener ---------------------------------
func (j *journal) OnNewData(c chunk.Chunk) {
	j.whenDataWritten()
}

func (j *journal) getLastChunk() chunk.Chunk {
	chunks := j.chunks.Load().(chunk.Chunks)
	if len(chunks) == 0 {
		j.logger.Debug("getLastChunk: chunks are empty")
		return nil
	}
	return chunks[len(chunks)-1]
}

func (j *journal) getWriterChunk(ctx context.Context, prevC chunk.Chunk) (chunk.Chunk, error) {
	if prevC == nil {
		prevC = j.getLastChunk()
		if prevC != nil {
			return prevC, nil
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case _, ok := <-j.wsem:
		if !ok {
			// the journal is closed
			return nil, util.ErrWrongState
		}
	}

	// here we have read j.wsem, only one go-routine could be here at a time
	var err error
	newChk := false
	chk := j.getLastChunk()
	var chunks chunk.Chunks
	if chk == nil || chk == prevC {
		chunks, err = j.cc.NewChunk(ctx, j)
		newChk = err == nil
	}

	j.lock.Lock()
	if !j.closed {
		if newChk {
			chk = chunks[len(chunks)-1]
			j.logger.Debug("New chunk is created ", chk)
			j.chunks.Store(chunks)
		}

		j.wsem <- true
	} else if err == nil {
		// if the chunk was created, but the journal is already closed
		err = util.ErrWrongState
	}
	j.lock.Unlock()
	return chk, err
}

// Called by chunk writer when synced
func (j *journal) whenDataWritten() {
	if atomic.LoadInt32(&j.waiters) <= 0 {
		return
	}

	j.lock.Lock()
	defer j.lock.Unlock()

	for i, ch := range j.dwChnls {
		close(ch)
		j.dwChnls[i] = nil
	}

	if cap(j.dwChnls) > 10 {
		j.dwChnls = nil
	} else {
		j.dwChnls = j.dwChnls[:0]
	}
}

// waitData allows to wait a new data is written into the journal. The function
// expects ctx context and curId a record Id to check the operation against to.
// The call will block current go routine, if the curId lies behind the last
// record in the journal. The case, the goroutine will be unblock until one of the
// two events happen - ctx is closed or new records added to the journal.
func (j *journal) waitData(ctx context.Context, curId Pos) error {
	atomic.AddInt32(&j.waiters, 1)
	defer atomic.AddInt32(&j.waiters, -1)

	for {
		j.lock.Lock()
		if j.closed {
			j.lock.Unlock()
			return util.ErrWrongState
		}

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

// getChunkById is looking for a chunk with cid. If there is no such chunk
// in the list, it will return the chunk with lowest Id, but which is greater
// then the cid. If there is no such chunks, so cid points is out
// of the chunks range, then the method returns nil
func (j *journal) getChunkById(cid chunk.Id) chunk.Chunk {
	chunks := j.chunks.Load().(chunk.Chunks)
	n := len(chunks)
	if n == 0 {
		return nil
	}

	idx := sort.Search(len(chunks), func(i int) bool { return chunks[i].Id() >= cid })
	// according to the condition idx is always in [0..n]
	if idx < n {
		return chunks[idx]
	}
	return nil
}
