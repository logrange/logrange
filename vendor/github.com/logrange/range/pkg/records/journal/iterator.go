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

package journal

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/chunk"
)

type (
	iterator struct {
		j     *journal
		pos   Pos
		ci    chunk.Iterator
		bkwrd bool
	}
)

func (it *iterator) SetBackward(bkwrd bool) {
	if it.bkwrd == bkwrd {
		return
	}
	it.bkwrd = bkwrd
	if it.ci != nil {
		it.ci.SetBackward(it.bkwrd)
	}
}

func (it *iterator) Next(ctx context.Context) {
	it.Get(ctx)
	if it.ci != nil {
		it.ci.Next(ctx)
		pos := it.ci.Pos()
		if pos < 0 {
			it.advanceChunk()
			return
		}
		it.pos.Idx = uint32(pos)
	}
}

func (it *iterator) Get(ctx context.Context) (records.Record, error) {
	err := it.ensureChkIt()
	if err != nil {
		return nil, err
	}

	rec, err := it.ci.Get(ctx)
	for err == io.EOF {
		err = it.advanceChunk()
		if err != nil {
			return nil, err
		}
		rec, err = it.ci.Get(ctx)
	}
	return rec, err
}

func (it *iterator) CurrentPos() records.IteratorPos {
	return it.Pos()
}

func (it *iterator) Pos() Pos {
	return it.pos
}

func (it *iterator) SetPos(pos Pos) {
	if pos == it.pos {
		return
	}

	if pos.CId != it.pos.CId {
		it.closeChunk()
	}

	if it.ci != nil {
		it.ci.SetPos(int64(pos.Idx))
	}
	it.pos = pos
}

func (it *iterator) Close() error {
	it.closeChunk()
	it.j = nil
	return nil
}

func (it *iterator) Release() {
	if it.ci != nil {
		it.ci.Release()
	}
}

func (it *iterator) closeChunk() {
	if it.ci != nil {
		it.ci.Close()
		it.ci = nil
	}
}

func (it *iterator) advanceChunk() error {
	it.closeChunk()
	if it.bkwrd {
		it.pos.CId--
		it.pos.Idx = math.MaxUint32
	} else {
		it.pos.CId++
		it.pos.Idx = 0
	}
	return it.ensureChkIt()
}

// ensureChkId selects chunk by position iterator. It corrects the position if needed
func (it *iterator) ensureChkIt() error {
	if it.ci != nil {
		return nil
	}

	var chk chunk.Chunk
	if it.bkwrd {
		chk = it.j.getChunkByIdOrLess(it.pos.CId)
	} else {
		chk = it.j.getChunkByIdOrGreater(it.pos.CId)
	}

	if chk == nil {
		return io.EOF
	}

	if chk.Id() < it.pos.CId {
		it.pos.CId = chk.Id()
		it.pos.Idx = chk.Count()
		if !it.bkwrd {
			return io.EOF
		}
	}

	if chk.Id() > it.pos.CId {
		it.pos.CId = chk.Id()
		it.pos.Idx = 0
	}

	var err error
	it.ci, err = chk.Iterator()
	if err != nil {
		return err
	}
	it.ci.SetBackward(it.bkwrd)
	it.ci.SetPos(int64(it.pos.Idx))
	it.pos.Idx = uint32(it.ci.Pos())
	return nil
}

func (it *iterator) String() string {
	return fmt.Sprintf("{pos=%s, ci exist=%t}", it.pos, it.ci != nil)
}
