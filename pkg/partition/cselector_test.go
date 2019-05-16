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
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tmindex"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/journal"
	"testing"
)

func TestChkSelectorGetNextPos(t *testing.T) {
	jrnl := &tstJournal{
		name: "tstJournal",
		cc: &tstChnksController{
			cks: []chunk.Chunk{
				&tstChunk{id: 10, cnt: 1000},
				&tstChunk{id: 20, cnt: 1000},
				&tstChunk{id: 30, cnt: 1000},
				&tstChunk{id: 40, cnt: 1000},
				&tstChunk{id: 50, cnt: 1000},
			},
		},
	}

	tsidxr := &tstTsIndexer{
		ris: []tmindex.RecordsInfo{
			{Id: 10, MinTs: 100, MaxTs: 190},
			{Id: 20, MinTs: 200, MaxTs: 290},
			{Id: 30, MinTs: 300, MaxTs: 390},
			{Id: 40, MinTs: 400, MaxTs: 490},
			{Id: 50, MinTs: 500, MaxTs: 590},
		},
		grEqMap: map[chunk.Id]uint32{20: 250, 40: 0},
		lessMap: map[chunk.Id]uint32{20: 999, 40: 450},
	}

	cs := newChkSelector(model.TimeRange{MinTs: 250, MaxTs: 450}, jrnl, tsidxr, &tstTmIndexRebuilder{})
	testCheckForwardPos(t, cs, journal.Pos{0, 0}, journal.Pos{20, 250})
	testCheckForwardPos(t, cs, journal.Pos{10, 9999}, journal.Pos{20, 250})
	testCheckForwardPos(t, cs, journal.Pos{12, 4}, journal.Pos{20, 250})
	testCheckForwardPos(t, cs, journal.Pos{20, 290}, journal.Pos{20, 290})
	testCheckForwardPos(t, cs, journal.Pos{20, 999}, journal.Pos{20, 999})
	testCheckForwardPos(t, cs, journal.Pos{20, 1000}, journal.Pos{30, 0})
	testCheckForwardPos(t, cs, journal.Pos{40, 23}, journal.Pos{40, 23})
	testCheckForwardPos(t, cs, journal.Pos{40, 449}, journal.Pos{40, 449})
	testCheckForwardPos(t, cs, journal.Pos{40, 450}, journal.Pos{40, 450})
	testCheckForwardPos(t, cs, journal.Pos{40, 451}, journal.Pos{0, 0})
	testCheckForwardPos(t, cs, journal.Pos{50, 450}, journal.Pos{0, 0})

	testCheckBackwardPos(t, cs, journal.Pos{0, 0}, journal.Pos{0, 0})
	testCheckBackwardPos(t, cs, journal.Pos{11, 30}, journal.Pos{0, 0})
	testCheckBackwardPos(t, cs, journal.Pos{20, 249}, journal.Pos{0, 0})
	testCheckBackwardPos(t, cs, journal.Pos{20, 250}, journal.Pos{20, 250})
	testCheckBackwardPos(t, cs, journal.Pos{30, 750}, journal.Pos{30, 750})
	testCheckBackwardPos(t, cs, journal.Pos{40, 450}, journal.Pos{40, 450})
	testCheckBackwardPos(t, cs, journal.Pos{40, 451}, journal.Pos{40, 450})
	testCheckBackwardPos(t, cs, journal.Pos{40, 890}, journal.Pos{40, 450})
	testCheckBackwardPos(t, cs, journal.Pos{60, 1450}, journal.Pos{40, 450})
}

func testCheckForwardPos(t *testing.T, cs *chkSelector, pos, expPos journal.Pos) {
	chk, chkSt, pos1, err := cs.getPosForward(context.Background(), pos)
	if err != nil {
		t.Fatal("Unexpected error for pos=", pos, ", expPos=", expPos, ", err=", err)
	}

	if chk == nil {
		if expPos.CId == 0 {
			return
		}
		t.Fatal("Expected chunk ", expPos, " for ", pos, ", but returned chk == nil ")
	}

	if cs.stats[chk.Id()] != chkSt {
		t.Fatal("Expecting to have ", *(cs.stats[chk.Id()]), ", but received ", *chkSt)
	}

	if pos1 != expPos {
		t.Fatal("Unexpected pos1=", pos1, ", for pos=", pos, ", expPos=", expPos)
	}
}

func testCheckBackwardPos(t *testing.T, cs *chkSelector, pos, expPos journal.Pos) {
	chk, chkSt, pos1, err := cs.getPosBackward(context.Background(), pos)
	if err != nil {
		t.Fatal("Backward: Unexpected error for pos=", pos, ", expPos=", expPos, ", err=", err)
	}

	if chk == nil {
		if expPos.CId == 0 {
			return
		}
		t.Fatal("Backward: Expected chunk ", expPos, " for ", pos, ", but returned chk == nil ")
	}

	if cs.stats[chk.Id()] != chkSt {
		t.Fatal("Expecting to have ", *(cs.stats[chk.Id()]), ", but received ", *chkSt)
	}

	if pos1 != expPos {
		t.Fatal("Backward: Unexpected pos1=", pos1, ", for pos=", pos, ", expPos=", expPos)
	}
}

type tstTmIndexRebuilder struct {
}

func (tir *tstTmIndexRebuilder) RebuildIndex(src string, cid chunk.Id, force bool) {

}
