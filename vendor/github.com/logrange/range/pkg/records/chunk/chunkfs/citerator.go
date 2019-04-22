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
	"io"
	"sync/atomic"

	"github.com/logrange/range/pkg/records"
)

type (
	// cIterator implements chunk.Iterator and it allows to iterate over records
	// forth (forward) and back (backward). The default iteration direction is forward.
	// The iterator has current position, which could be in the range [-1 .. Count] where
	// the Count is number of records in the chunk.
	//
	// Get's behavior together with the current iterator position could give different results
	// depending on the direction of the iterator. The following table shows what Get() will return:
	//
	//      pos    |    direction    |  Get returns:
	// ------------+-----------------+---------------
	//      < 0    |     FORWARD     | First record
	//      < 0    |     BACKWARD    | io.EOF
	//   >= Count  |     FORWARD     | io.EOF
	//   >= Count  |     BACKWARD    | Last record
	// [0 .. Count)|       ANY       | The record at the pos
	//
	cIterator struct {
		gid    uint64
		fdPool *FdPool
		cr     cReader
		cnt    *uint32
		pos    int64
		buf    []byte
		res    []byte
		bkwd   bool

		// cached values to acquire readers
		doffs int64
	}
)

func newCIterator(gid uint64, fdPool *FdPool, count *uint32, buf []byte) *cIterator {
	ci := new(cIterator)
	ci.gid = gid
	ci.fdPool = fdPool
	ci.cnt = count
	ci.pos = 0
	ci.bkwd = false
	ci.buf = buf[:cap(buf)]
	return ci
}

// Close is part of io.Closer
func (ci *cIterator) Close() error {
	ci.Release()
	ci.fdPool = nil
	ci.buf = nil
	return nil
}

// Next is part of records.Iterator
func (ci *cIterator) Next(ctx context.Context) {
	_, err := ci.Get(ctx)
	if err == nil {
		if ci.bkwd {
			ci.SetPos(ci.pos - 1)
		} else {
			ci.pos++
		}
	}
	ci.res = nil
}

// Get returns current iterator record.
//
// NOTE: for the implementation returns record in the internal buffer, which
// can be overwritten after Next(). Invoker must copy record if needed or dis-
// regard value return by the Get after calling Next() for the iterator object
func (ci *cIterator) Get(ctx context.Context) (records.Record, error) {
	if ci.res != nil {
		return ci.res, nil
	}

	err := ci.ensureFileReader(ctx)
	if err != nil {
		return nil, err
	}

	ci.doffs = ci.cr.dr.getNextReadPos()
	ci.res, err = ci.cr.readRecord(ci.buf)
	if err != nil {
		ci.Release()
	}

	return ci.res, err
}

// Release is part of chunk.Iterator
func (ci *cIterator) Release() {
	if ci.cr.dr != nil {
		ci.fdPool.release(ci.cr.dr)
		ci.cr.dr = nil
	}

	if ci.cr.ir != nil {
		ci.fdPool.release(ci.cr.ir)
		ci.cr.ir = nil
	}
	ci.res = nil
}

func (ci *cIterator) CurrentPos() records.IteratorPos {
	return ci.Pos()
}

// Pos is part of chunk.Iterator
func (ci *cIterator) Pos() int64 {
	return ci.pos
}

// SetPos sets the cIterator next reading position. The pos is allowed to be in the
// range [-1 .. Count] where Count is the number of records in the chunk
func (ci *cIterator) SetPos(pos int64) error {
	if pos == ci.pos {
		return nil
	}

	cnt := int64(atomic.LoadUint32(ci.cnt))
	if pos > cnt {
		pos = cnt
	}

	if pos < 0 {
		pos = -1
	}

	ci.pos = pos
	ci.res = nil

	if ci.bkwd {
		return ci.cr.setPosBackward(ci.pos)
	}

	return ci.cr.setPos(ci.pos)
}

// SetBackward is part of chunk.Iterator
func (ci *cIterator) SetBackward(bkwd bool) {
	ci.bkwd = bkwd
}

func (ci *cIterator) ensureFileReader(ctx context.Context) error {
	cnt := int64(atomic.LoadUint32(ci.cnt))
	if ci.bkwd {
		if ci.pos >= cnt {
			ci.SetPos(cnt - 1)
		}
	} else {
		if ci.pos < 0 {
			ci.SetPos(0)
		}
	}

	if ci.pos < 0 || ci.pos >= cnt {
		return io.EOF
	}

	var err error
	if ci.cr.dr == nil {
		ci.cr.dr, err = ci.fdPool.acquire(ctx, ci.gid, ci.doffs)

		if ci.cr.ir == nil && err == nil {
			ci.cr.ir, err = ci.fdPool.acquire(ctx, ci.gid+1, int64(ci.pos)*int64(ChnkIndexRecSize))
		}

		if err == nil {
			err = ci.cr.setPos(ci.pos)
		} else {
			ci.Release()
		}
	}

	return err
}
