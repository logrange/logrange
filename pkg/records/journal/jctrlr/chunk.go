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
	"context"
	"fmt"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
	lsync "github.com/logrange/logrange/pkg/sync"
)

type (
	cDesc struct {
		cid   chunk.Id
		chunk *chunkWrapper
		state int
	}

	chunkWrapper struct {
		rwLock lsync.RWLock
		chunk  chunk.Chunk
	}

	chunkIteratorWrapper struct {
		cw *chunkWrapper
		ci chunk.Iterator
	}
)

const (
	cStateNew      = 0
	cStateChecking = 1
	cStateError    = 2
	cStateReady    = 3
	cStateHold     = 4
	cStateDeleted  = 5
)

func newCDesc(cid chunk.Id) *cDesc {
	cd := new(cDesc)
	cd.cid = cid
	cd.state = cStateNew
	return cd
}

func (cd *cDesc) String() string {
	return fmt.Sprintf("{cid=%s, state=%s, chunk=%s}", cd.cid, cDescStateName(cd.state), cd.chunk)
}

func (cd *cDesc) Close() error {
	if cd.chunk != nil {
		cd.chunk.closeInternal()
		cd.chunk = nil
	}
	return nil
}

func cDescStateName(state int) string {
	switch state {
	case cStateNew:
		return "NEW"
	case cStateChecking:
		return "CHECKING"
	case cStateError:
		return "ERROR"
	case cStateReady:
		return "READY"
	case cStateHold:
		return "HOLD"
	case cStateDeleted:
		return "DELETED"
	}
	return "N/A"
}

// ----------------------------- chunkWrapper -------------------------------
func (cw *chunkWrapper) Close() error {
	panic("Close must not be called on the wrapper. closeInternal must be used.")
}

func (cw *chunkWrapper) String() string {
	if cw.chunk == nil {
		return "nil"
	}
	return cw.chunk
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
	if err != nil {
		cw.rwLock.RUnlock()
		return nil, err
	}

	return &chunkIteratorWrapper{cw, ci}, nil
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
	ci.cw.rwLock.RUnlock()
	ci.cw = nil
	return ci.ci.Close()
}

func (ci *chunkIteratorWrapper) Release() {
	ci.ci.Release()
}

func (ci *chunkIteratorWrapper) Pos() uint32 {
	return ci.ci.Pos()
}

func (ci *chunkIteratorWrapper) SetPos(pos uint32) error {
	return ci.ci.SetPos(pos)
}

func (ci *chunkIteratorWrapper) Next(ctx context.Context) {
	ci.ci.Next(ctx)
}

func (ci *chunkIteratorWrapper) Get(ctx context.Context) (records.Record, error) {
	return ci.ci.Get(ctx)
}
