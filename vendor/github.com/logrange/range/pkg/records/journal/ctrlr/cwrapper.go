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

package ctrlr

import (
	"context"
	"fmt"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/chunk"
	lsync "github.com/logrange/range/pkg/sync"
)

type (
	chunkWrapper struct {
		rwLock lsync.RWLock
		chunk  chunk.Chunk
	}

	chunkIteratorWrapper struct {
		cw   *chunkWrapper
		ci   chunk.Iterator
		lock bool
	}
)

// ----------------------------- chunkWrapper -------------------------------
func (cw *chunkWrapper) Close() error {
	panic("Close must not be called on the wrapper. closeInternal must be used.")
}

func (cw *chunkWrapper) String() string {
	if cw.chunk == nil {
		return "nil"
	}
	return fmt.Sprint(cw.chunk)
}

func (cw *chunkWrapper) Id() chunk.Id {
	return cw.chunk.Id()
}

func (cw *chunkWrapper) Write(ctx context.Context, it records.Iterator) (int, uint32, error) {
	if err := cw.rwLock.RLockWithCtx(ctx); err != nil {
		return 0, 0, err
	}
	n, off, err := cw.chunk.Write(ctx, it)
	cw.rwLock.RUnlock()
	return n, off, err
}

func (cw *chunkWrapper) Sync() {
	cw.chunk.Sync()
}

func (cw *chunkWrapper) Iterator() (chunk.Iterator, error) {
	if err := cw.rwLock.RLock(); err != nil {
		return nil, err
	}

	ci, err := cw.chunk.Iterator()
	cw.rwLock.RUnlock()
	if err != nil {
		return nil, err
	}

	return &chunkIteratorWrapper{cw, ci, false}, nil
}

func (cw *chunkWrapper) Size() int64 {
	return cw.chunk.Size()
}

func (cw *chunkWrapper) Count() uint32 {
	return cw.chunk.Count()
}

func (cw *chunkWrapper) AddListener(lstnr chunk.Listener) {
	cw.chunk.AddListener(lstnr)
}

func (cw *chunkWrapper) closeInternal() error {
	cw.rwLock.Close()
	err := cw.chunk.Close()
	cw.chunk = nil
	return err
}

// ------------------------ chunkIteratorWrapper -----------------------------

func (ci *chunkIteratorWrapper) Close() error {
	ci.unlock()
	ci.cw = nil
	return ci.ci.Close()
}

// Release unlocks the read lock
func (ci *chunkIteratorWrapper) Release() {
	ci.ci.Release()
	ci.unlock()
}

func (ci *chunkIteratorWrapper) Pos() int64 {
	if ci.ensureLock() != nil {
		return 0
	}
	return ci.ci.Pos()
}

func (ci *chunkIteratorWrapper) SetPos(pos int64) error {
	if err := ci.ensureLock(); err != nil {
		return err
	}
	return ci.ci.SetPos(pos)
}

func (ci *chunkIteratorWrapper) Next(ctx context.Context) {
	if ci.ensureLock() != nil {
		return
	}
	ci.ci.Next(ctx)
}

func (ci *chunkIteratorWrapper) Get(ctx context.Context) (records.Record, error) {
	if err := ci.ensureLock(); err != nil {
		return nil, err
	}
	return ci.ci.Get(ctx)
}

func (ci *chunkIteratorWrapper) SetBackward(bkwd bool) {
	ci.ci.SetBackward(bkwd)
}

func (ci *chunkIteratorWrapper) CurrentPos() records.IteratorPos {
	return ci.ci.Pos()
}

func (ci *chunkIteratorWrapper) ensureLock() error {
	if ci.lock {
		return nil
	}
	return ci.rlock()
}

func (ci *chunkIteratorWrapper) rlock() error {
	err := ci.cw.rwLock.RLock()
	ci.lock = err == nil
	return err
}

func (ci *chunkIteratorWrapper) unlock() {
	if ci.lock {
		ci.cw.rwLock.RUnlock()
		ci.lock = false
	}
}
