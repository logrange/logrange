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
	"io"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	iterator struct {
		j   *journal
		pos Pos
		ci  chunk.Iterator
	}
)

func (it *iterator) Next(ctx context.Context) {
	it.Get(ctx)
	if it.ci != nil {
		it.ci.Next(ctx)
		it.pos.Idx = it.ci.Pos()
	}
}

func (it *iterator) Get(ctx context.Context) (records.Record, error) {
	err := it.ensureChkIt(it.pos.CId)
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

func (it *iterator) Pos() Pos {
	return it.pos
}

func (it *iterator) SetPos(pos Pos) {
	if pos == it.pos {
		return
	}

	if pos.CId != it.pos.CId {
		it.Release()
	}

	if it.ci != nil {
		it.ci.SetPos(pos.Idx)
	}
	it.pos = pos
}

func (it *iterator) Close() error {
	it.Release()
	it.j = nil
	return nil
}

func (it *iterator) Release() {
	if it.ci != nil {
		it.ci.Close()
		it.ci = nil
	}
}

func (it *iterator) advanceChunk() error {
	it.Release()
	it.pos.CId++
	return it.ensureChkIt(it.pos.CId)
}

// findChunkByPos looks for the chunk and updates the pos if needed
func (it *iterator) findChkAndCorrectPos(cid chunk.Id) chunk.Chunk {
	chk := it.j.getChunkById(cid)
	if chk == nil {
		return nil
	}

	it.pos.CId = chk.Id()
	if chk.Id() > cid {
		it.pos.Idx = 0
	}

	return chk
}

func (it *iterator) ensureChkIt(cid chunk.Id) error {
	if it.ci != nil {
		return nil
	}

	chk := it.findChkAndCorrectPos(cid)
	if chk == nil {
		return io.EOF
	}

	var err error
	it.ci, err = chk.Iterator()
	if err != nil {
		return err
	}
	it.ci.SetPos(it.pos.Idx)
	it.pos.Idx = it.ci.Pos()
	return nil
}
