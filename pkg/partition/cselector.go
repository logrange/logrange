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
	"math"
)

type (
	// chkSelector allows to select appropriate journal's chunk taking into account
	// the time range for the records selection. This structure is used by Journal
	// iterator to calculate next read postion (chunk and offset) effectively.
	// The chkSelector is constructed with the range of times, the selector works with.
	// Every journal chunk in context of the selector, has the state, which describes
	// available positions for the chunk (see chkStatus)
	chkSelector struct {
		tmRange     model.TimeRange
		jrnl        journal.Journal
		tmidx       tmindex.TsIndexer
		tiRebuilder TmIndexRebuilder
		stats       map[chunk.Id]*chkStatus
	}

	// chkStatus struct describes allowed time positions in the chunk. 5 possible
	// combinations [minPos .. maxPos] are possible ( 0 < x < count):
	//
	// 1) [maxUnit32 .. maxUint32]  - all records are out of the range
	// 2) [x .. maxUint32] 		- records starting from the position x are allowed
	// 3) [0 .. x] 				- records from the position 0 to position x are allowed
	// 4) [0 .. maxUInt32]  	- all records from the chunka are allowed
	// 5) [x0 .. x1] 			- only records from position x0 to position x1 are allowed
	chkStatus struct {
		minPos uint32
		maxPos uint32
		count  uint32
	}
)

func newChkSelector(tmRange model.TimeRange, jrnl journal.Journal, tmidx tmindex.TsIndexer, tiRebuilder TmIndexRebuilder) *chkSelector {
	cs := new(chkSelector)
	cs.tmRange = tmRange
	cs.jrnl = jrnl
	cs.tmidx = tmidx
	cs.tiRebuilder = tiRebuilder
	return cs
}

// getPosForward checks position and forward it if needed. It also returns the chunk
// which corresponds to the new position if it is changed.
//
// If the new pos is out of last record, it returns nil, nil, <last record of last chunk>, nil
// If an error happens, it returns err != nil
func (cs *chkSelector) getPosForward(ctx context.Context, pos journal.Pos) (chunk.Chunk, *chkStatus, journal.Pos, error) {
	cks, err := cs.jrnl.Chunks().Chunks(ctx)
	n := len(cks)
	if err != nil || n == 0 {
		return nil, nil, journal.Pos{}, err
	}

	idx := getChunkByIdOrGreater(cks, pos.CId)
	var chk chunk.Chunk
	if idx == n {
		// well, it seems like we are out of range. returning last record.
		chk = cks[idx-1]
		return nil, nil, journal.Pos{chk.Id(), chk.Count()}, nil
	}

	pIdx := pos.Idx
	if cks[idx].Id() != pos.CId {
		// returned chunk is after the searched one
		pIdx = 0
	}

	for ; idx < n; idx++ {
		chk = cks[idx]
		chkSt := cs.getChunkStatus(ctx, chk, cks)
		if np, ok := chkSt.checkPosOrAdvance(pIdx); ok {
			return chk, chkSt, journal.Pos{chk.Id(), np}, nil
		}
		pIdx = 0
	}

	// ok, the last one
	return nil, nil, journal.Pos{chk.Id(), chk.Count()}, nil
}

// getPosBackward checks position and move it backward if it is needed. It also returns the chunk
// which corresponds to the new position if it is changed.
//
// If the next pos is less than the position of first record, it returns nil, nil, <first record of first chunk>, nil
// If an error happens, it returns err != nil
func (cs *chkSelector) getPosBackward(ctx context.Context, pos journal.Pos) (chunk.Chunk, *chkStatus, journal.Pos, error) {
	cks, err := cs.jrnl.Chunks().Chunks(ctx)
	n := len(cks)
	if err != nil || n == 0 {
		return nil, nil, journal.Pos{}, err
	}

	idx := getChunkByIdOrLess(cks, pos.CId)
	var chk chunk.Chunk
	if idx == -1 {
		// well, it seems like we are out of range. returning the first record.
		chk = cks[0]
		return nil, nil, journal.Pos{chk.Id(), 0}, nil
	}

	pIdx := pos.Idx
	if cks[idx].Id() != pos.CId {
		// returned chunk is after the searched one
		pIdx = cks[idx].Count() - 1
	}

	for ; idx >= 0; idx-- {
		chk = cks[idx]
		chkSt := cs.getChunkStatus(ctx, chk, cks)
		if pp, ok := chkSt.checkPosOrReduce(pIdx); ok {
			return chk, chkSt, journal.Pos{chk.Id(), pp}, nil
		}
		pIdx = math.MaxUint32
	}

	// ok, the last one
	return nil, nil, journal.Pos{chk.Id(), 0}, nil
}

func (cs *chkSelector) getChunkStatus(ctx context.Context, chk chunk.Chunk, cks chunk.Chunks) *chkStatus {
	chkSt, ok := cs.stats[chk.Id()]
	if !ok || len(cks) != len(cs.stats) {
		cs.rebuildChunkStatuses(ctx, cks)
		chkSt = cs.stats[chk.Id()]
	} else if chkSt.count != chk.Count() {
		chkSt.count = chk.Count()
		ri, err := cs.tmidx.GetRecordsInfo(cs.jrnl.Name(), chk.Id())
		if err == nil {
			cs.updatePoss(chk, chkSt, ri)
		}
	}
	return chkSt
}

func (cs *chkSelector) rebuildChunkStatuses(ctx context.Context, cks chunk.Chunks) {
	ris := cs.tmidx.SyncChunks(ctx, cs.jrnl.Name(), cks)
	nm := make(map[chunk.Id]*chkStatus, len(cks))
	for i, ri := range ris {
		chkSt := cs.stats[ri.Id]
		if chkSt == nil {
			chkSt = &chkStatus{}
		}

		ch := cks[i]
		chkSt.count = ch.Count()
		cs.updatePoss(ch, chkSt, ri)
		nm[ri.Id] = chkSt
	}
	cs.stats = nm
}

func (cs *chkSelector) updatePoss(ch chunk.Chunk, chkSt *chkStatus, ri tmindex.RecordsInfo) {
	if cs.tmRange.MaxTs < ri.MinTs || cs.tmRange.MinTs > ri.MaxTs {
		// case 1
		chkSt.minPos, chkSt.maxPos = math.MaxUint32, math.MaxUint32
	} else {
		chkSt.minPos = 0
		chkSt.maxPos = math.MaxUint32

		var err error
		if cs.tmRange.MinTs >= ri.MinTs {
			// case 5
			chkSt.minPos, err = cs.tmidx.GetPosForGreaterOrEqualTime(cs.jrnl.Name(), ri.Id, cs.tmRange.MinTs)
			if err != nil {
				chkSt.minPos = 0
				cs.tiRebuilder.RebuildIndex(cs.jrnl.Name(), ch.Id(), false)
			}
		}

		if cs.tmRange.MaxTs <= ri.MaxTs {
			chkSt.maxPos, err = cs.tmidx.GetPosForLessTime(cs.jrnl.Name(), ri.Id, cs.tmRange.MaxTs)
			if err != nil {
				chkSt.maxPos = math.MaxUint32
				cs.tiRebuilder.RebuildIndex(cs.jrnl.Name(), ch.Id(), false)
			}
		}
	}
}

// checkPosOrAdvance checks the provided position and adjust it for the chunk range if needed.
// it returns new position and flag whether the position is within the valid records range or not
func (chkSt *chkStatus) checkPosOrAdvance(pos uint32) (uint32, bool) {
	if pos < chkSt.minPos {
		// pull up the pos to minPos
		pos = chkSt.minPos
	}

	res := true
	if pos >= chkSt.count || pos > chkSt.maxPos {
		pos = chkSt.count
		res = false
	}

	return pos, res
}

// checkPosToReduce checks the provided position and ajust it for the chunk range if it is needed.
// it returns the corrected position and flag whether the position makes sense (true) or it is out of the
// range(false)
func (chkSt *chkStatus) checkPosOrReduce(pos uint32) (uint32, bool) {
	if pos > chkSt.maxPos {
		// reduce the pos to maxPos
		pos = chkSt.maxPos
	}

	if pos >= chkSt.count {
		pos = chkSt.count - 1
	}

	return pos, pos >= chkSt.minPos && chkSt.count > 0
}

// getChunkByIdOrGreater returns index of first chunk for which Id >= cid.
// returned value is in between [0..len(chunks)]
func getChunkByIdOrGreater(chunks chunk.Chunks, cid chunk.Id) int {
	i, j := 0, len(chunks)
	for i < j {
		h := int(uint(i+j) >> 1)
		if chunks[h].Id() < cid {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

// getChunkByIdOrLess returns last index of chunk which Id <= cid.
// the returned value is in [-1 .. len(chunks)], where  -1 is returned
// if chunks[0].Id() > cid
func getChunkByIdOrLess(chunks chunk.Chunks, cid chunk.Id) int {
	idx := getChunkByIdOrGreater(chunks, cid)
	if idx == len(chunks) || chunks[idx].Id() > cid {
		idx--
	}
	return idx
}
