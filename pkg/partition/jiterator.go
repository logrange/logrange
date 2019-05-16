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

package partition

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tmindex"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/journal"
	"io"
	"math"
)

type (
	// JIterator struct provides records.Iterator implementation on top of a
	// journal.Journal. The implementation allows to use an position selector
	// solution (chkSelector) which uses time-index itself for smart positioning
	// in the iterator.
	JIterator struct {
		j     journal.Journal
		pos   journal.Pos
		ci    chunk.Iterator
		chkSt *chkStatus
		cs    *chkSelector
		bkwrd bool
	}
)

// NewJIterator constructs new JIterator instance
func NewJIterator(tmRange model.TimeRange, jrnl journal.Journal, tmidx tmindex.TsIndexer, tmRebuilder TmIndexRebuilder) *JIterator {
	jit := new(JIterator)
	jit.j = jrnl
	jit.cs = newChkSelector(tmRange, jrnl, tmidx, tmRebuilder)
	return jit
}

func (jit *JIterator) SetBackward(bkwrd bool) {
	if jit.bkwrd == bkwrd {
		return
	}
	jit.bkwrd = bkwrd
	if jit.ci != nil {
		jit.ci.SetBackward(jit.bkwrd)
	}
}

func (jit *JIterator) Next(ctx context.Context) {
	jit.Get(ctx)
	if jit.ci != nil {
		jit.ci.Next(ctx)
		pos := jit.ci.Pos()
		if pos < 0 || uint32(pos) < jit.chkSt.minPos || uint32(pos) > jit.chkSt.maxPos {
			jit.advanceChunk(ctx)
			return
		}
		jit.pos.Idx = uint32(pos)
	}
}

func (jit *JIterator) Get(ctx context.Context) (records.Record, error) {
	err := jit.ensureChkIt(ctx)
	if err != nil {
		return nil, err
	}

	rec, err := jit.ci.Get(ctx)
	for err == io.EOF {
		err = jit.advanceChunk(ctx)
		if err != nil {
			return nil, err
		}
		rec, err = jit.ci.Get(ctx)
	}
	return rec, err
}

func (jit *JIterator) CurrentPos() records.IteratorPos {
	return jit.Pos()
}

func (jit *JIterator) Pos() journal.Pos {
	return jit.pos
}

func (jit *JIterator) SetPos(pos journal.Pos) {
	if pos == jit.pos {
		return
	}

	if pos.CId != jit.pos.CId {
		jit.closeChunk()
	}

	if jit.ci != nil {
		jit.ci.SetPos(int64(pos.Idx))
	}

	jit.pos = pos
}

func (jit *JIterator) Close() error {
	jit.closeChunk()
	jit.j = nil
	return nil
}

func (jit *JIterator) Release() {
	if jit.ci != nil {
		jit.ci.Release()
	}
}

func (jit *JIterator) closeChunk() {
	if jit.ci != nil {
		jit.ci.Close()
		jit.ci = nil
		jit.chkSt = nil
	}
}

func (jit *JIterator) advanceChunk(ctx context.Context) error {
	jit.closeChunk()
	if jit.bkwrd {
		jit.pos.CId--
		jit.pos.Idx = math.MaxUint32
	} else {
		jit.pos.CId++
		jit.pos.Idx = 0
	}
	return jit.ensureChkIt(ctx)
}

// ensureChkId selects chunk by position iterator. It corrects the position if needed
func (jit *JIterator) ensureChkIt(ctx context.Context) error {
	if jit.ci != nil {
		return nil
	}

	var chk chunk.Chunk
	var err error
	var chkSt *chkStatus
	if jit.bkwrd {
		chk, chkSt, jit.pos, err = jit.cs.getPosBackward(ctx, jit.pos)
	} else {
		chk, chkSt, jit.pos, err = jit.cs.getPosForward(ctx, jit.pos)
	}

	if err != nil {
		return err
	}

	if chk == nil {
		return io.EOF
	}

	jit.ci, err = chk.Iterator()
	if err != nil {
		return err
	}

	jit.chkSt = chkSt
	jit.ci.SetBackward(jit.bkwrd)
	jit.ci.SetPos(int64(jit.pos.Idx))
	jit.pos.Idx = uint32(jit.ci.Pos())
	return nil
}

func (jit *JIterator) String() string {
	return fmt.Sprintf("{pos=%s, ci exist=%t}", jit.pos, jit.ci != nil)
}
