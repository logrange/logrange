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

package tmindex

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	"github.com/logrange/range/pkg/utils/errors"
	"io/ioutil"
	"math"
	"os"
	"testing"
)

// tstCtx struct keeps some data for tests
type tstCtx struct {
	dir  string
	p    *chunkfs.FdPool
	cks  []chunk.Chunk
	tidx *tmidx
}

func TestTsIndexerGrEqPos(t *testing.T) {
	tc := newTstCtx(t)
	defer tc.Close()

	// fill timestamps from [500..5500]
	for i := 0; i < 10; i++ {
		err := tc.tidx.OnWrite("a", uint32(i*sparseSpace), uint32((i+1)*sparseSpace), RecordsInfo{1, int64(sparseSpace*i + sparseSpace), int64(sparseSpace*i + sparseSpace*2)})
		if err != nil {
			t.Fatal("could not write err=", err)
		}
	}

	_, err := tc.tidx.GetPosForGreaterOrEqualTime("bb", 1, 1)
	if err != errors.NotFound {
		t.Fatal(" must be not found, but err=", err)
	}

	_, err = tc.tidx.GetPosForGreaterOrEqualTime("a", 2, 1)
	if err != errors.NotFound {
		t.Fatal(" must be not found, but err=", err)
	}

	tc.checkGrEqPos(t, "a", 1, sparseSpace/2, 0)
	tc.checkGrEqPos(t, "a", 1, sparseSpace, 0)
	tc.checkGrEqPos(t, "a", 1, sparseSpace+sparseSpace/2, 0)
	tc.checkGrEqPos(t, "a", 1, sparseSpace*2, sparseSpace)
	tc.checkGrEqPos(t, "a", 1, sparseSpace*2+sparseSpace/2, sparseSpace)
	tc.checkGrEqPos(t, "a", 1, sparseSpace*3-1, sparseSpace)
	tc.checkGrEqPos(t, "a", 1, sparseSpace*3, sparseSpace*2)
	tc.checkGrEqPos(t, "a", 1, sparseSpace*10, sparseSpace*9)
	tc.checkGrEqPos(t, "a", 1, sparseSpace*300, -1)
}

func TestTsIndexerLessPosAndLastRecord(t *testing.T) {
	tc := newTstCtx(t)
	defer tc.Close()

	// fill timestamps from [500..5500] for pos [0..5000]
	// ts = 500 - pos 0
	// ts = 1000 - pos 500
	for i := 0; i < 10; i++ {
		err := tc.tidx.OnWrite("a", uint32(i*sparseSpace), uint32((i+1)*sparseSpace), RecordsInfo{1, int64(sparseSpace*i + sparseSpace), int64(sparseSpace*i + sparseSpace*2)})
		if err != nil {
			t.Fatal("could not write err=", err)
		}

		ri, err := tc.tidx.LastChunkRecordsInfo("a")
		if err != nil {
			t.Fatal("Last record must be returned no problems")
		}
		if ri.MinTs != sparseSpace || ri.MaxTs != int64(sparseSpace*i+sparseSpace*2) {
			t.Fatal("unexpecter last record info ", ri, " for i=", i)
		}
	}

	_, err := tc.tidx.GetPosForLessTime("bb", 1, 1)
	if err != errors.NotFound {
		t.Fatal(" must be not found, but err=", err)
	}

	_, err = tc.tidx.GetPosForLessTime("a", 2, 1)
	if err != errors.NotFound {
		t.Fatal(" must be not found, but err=", err)
	}

	tc.checkLessPos(t, "a", 1, 50, -1)
	tc.checkLessPos(t, "a", 1, sparseSpace-1, -1)
	tc.checkLessPos(t, "a", 1, sparseSpace, -1)
	tc.checkLessPos(t, "a", 1, sparseSpace+1, sparseSpace)
	tc.checkLessPos(t, "a", 1, sparseSpace+50, sparseSpace)
	tc.checkLessPos(t, "a", 1, sparseSpace*2, 2*sparseSpace)
	tc.checkLessPos(t, "a", 1, sparseSpace*2+1, sparseSpace*2)
	tc.checkLessPos(t, "a", 1, sparseSpace*2+100, sparseSpace*2)
	tc.checkLessPos(t, "a", 1, sparseSpace*10, sparseSpace*10)
	tc.checkLessPos(t, "a", 1, sparseSpace*11, math.MaxUint32)
	tc.checkLessPos(t, "a", 1, sparseSpace*111, math.MaxUint32)
}

func TestTsIndexerOnWriteBigGap(t *testing.T) {
	tc := newTstCtx(t)
	defer tc.Close()

	err := tc.tidx.OnWrite("a", uint32(30000), uint32(30010), RecordsInfo{1, int64(4400), int64(4500)})
	if err != ErrTmIndexCorrupted {
		t.Fatal("chunk 1: must be an error due to the big gap, but err=", err)
	}

	err = tc.tidx.OnWrite("a", uint32(20000), uint32(20010), RecordsInfo{2, int64(4400), int64(4500)})
	if err != ErrTmIndexCorrupted {
		t.Fatal("chunk 2: must be an error due to the big gap, but err=", err)
	}
}

func TestTsIndexerSyncChunks(t *testing.T) {
	tc := newTstCtx(t)
	defer tc.Close()

	c := tc.newChunk(t)
	err := writeDataToChunk(c, 1, 10000)
	if err != nil {
		t.Fatal("could not write data to chunk ", c.Id())
	}

	ris := tc.tidx.SyncChunks(context.Background(), "a", chunk.Chunks{c})
	if len(ris) != 1 {
		t.Fatal("must be 1 chunk!, but ", len(ris))
	}
	ri := ris[0]
	if ri.Id != c.Id() || ri.MinTs != 1 || ri.MaxTs != 10000 {
		t.Fatal("unexpected RecordsInfo ", ri)
	}

	lri, err := tc.tidx.LastChunkRecordsInfo("a")
	if err != nil || lri != ri {
		t.Fatal("lri=", lri, " but expected ", ri, ", or err=", err)
	}

	pos, err := tc.tidx.GetPosForGreaterOrEqualTime("a", c.Id(), 400)
	if err != ErrTmIndexCorrupted {
		t.Fatal("expecting ErrTmIndexCorrupted, but err=", err, ", pos=", pos)
	}

	err = tc.tidx.OnWrite("a", uint32(10001), uint32(10010), RecordsInfo{c.Id(), int64(10001), int64(10010)})
	if err != ErrTmIndexCorrupted {
		t.Fatal("must be corrupted index")
	}

	// add more ...
	ris = tc.tidx.SyncChunks(context.Background(), "a", chunk.Chunks{c})
	ri = ris[0]
	if ri.Id != c.Id() || ri.MinTs != 1 || ri.MaxTs != 10010 {
		t.Fatal("unexpected RecordsInfo ", ri)
	}
}

func TestTsIndexerRebuildIndex(t *testing.T) {
	tc := newTstCtx(t)
	defer tc.Close()

	c := tc.newChunk(t)
	err := writeDataToChunk(c, 1, 10000)
	if err != nil {
		t.Fatal("could not write data to chunk ", c.Id())
	}

	ris := tc.tidx.SyncChunks(context.Background(), "a", chunk.Chunks{c})
	if len(ris) != 1 {
		t.Fatal("must be 1 chunk!, but ", len(ris))
	}
	ri := ris[0]
	if ri.Id != c.Id() || ri.MinTs != 1 || ri.MaxTs != 10000 {
		t.Fatal("unexpected RecordsInfo ", ri)
	}

	lri, err := tc.tidx.LastChunkRecordsInfo("a")
	if err != nil || lri != ri {
		t.Fatal("lri=", lri, " but expected ", ri, ", or err=", err)
	}

	pos, err := tc.tidx.GetPosForGreaterOrEqualTime("a", c.Id(), 400)
	if err != ErrTmIndexCorrupted {
		t.Fatal("expecting ErrTmIndexCorrupted, but err=", err, ", pos=", pos)
	}

	tc.tidx.RebuildIndex(context.Background(), "a", c, false)
	pos, err = tc.tidx.GetPosForGreaterOrEqualTime("a", c.Id(), 100)
	if err != nil || pos != 0 {
		t.Fatal("expecting pos=0 and no err, but err=", err, ", pos=", pos)
	}

	pos, err = tc.tidx.GetPosForGreaterOrEqualTime("a", c.Id(), 5001)
	if err != nil || pos != 5000 {
		t.Fatal("expecting pos=5000 and no err, but err=", err, ", pos=", pos)
	}

	// now make it corrupted
	err = tc.tidx.OnWrite("a", uint32(30001), uint32(30010), RecordsInfo{c.Id(), int64(30001), int64(30010)})
	if err != nil {
		t.Fatal("must not be an error, but err=", err)
	}

	lri, err = tc.tidx.LastChunkRecordsInfo("a")
	if err != nil || lri.MaxTs != 30010 {
		t.Fatal("lri=", lri, " but expected MaxTs=30010, or err=", err)
	}

	pos, err = tc.tidx.GetPosForGreaterOrEqualTime("a", c.Id(), 501)
	if err != nil || pos != 500 {
		t.Fatal("expecting no error or pos=500, but err=", err, ", pos=", pos)
	}
	tc.tidx.RebuildIndex(context.Background(), "a", c, true)
	tc.tidx.RebuildIndex(context.Background(), "a", c, true)

	pos, err = tc.tidx.GetPosForGreaterOrEqualTime("a", c.Id(), 5001)
	if err != nil || pos != 5000 {
		t.Fatal("expecting pos=5000 and no err, but err=", err, ", pos=", pos)
	}
}

func writeDataToChunk(c chunk.Chunk, startTime int64, recs int) error {
	levs := make([]model.LogEvent, recs)
	for i := 0; i < recs; i++ {
		levs[i] = model.LogEvent{Timestamp: startTime, Msg: []byte(fmt.Sprintf("msg%d", i)), Fields: ""}
		startTime++
	}

	lew := model.NewTestLogEventsWrapper(levs)
	n, _, err := c.Write(context.Background(), lew)
	if err == nil {
		if n != recs {
			err = fmt.Errorf("n=%d is not equal to expected %d", n, recs)
		}
	}
	c.Sync()
	return err
}

func newTstCtx(t *testing.T) *tstCtx {
	tc := new(tstCtx)
	var err error
	tc.dir, err = ioutil.TempDir("", "chunkTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}

	tc.p = chunkfs.NewFdPool(20)
	tc.cks = make([]chunk.Chunk, 0, 1)

	tidx := NewTsIndexer().(*tmidx)
	tidx.Config = &TsIndexerConfig{tc.dir}
	err = tidx.Init(nil)
	if err != nil {
		t.Fatal("could not create tmidx err=", err)
	}
	tc.tidx = tidx

	return tc
}

func (tc *tstCtx) newChunk(t *testing.T) *chunkfs.Chunk {
	fn := chunkfs.MakeChunkFileName(tc.dir, chunk.NewId())
	cfg := chunkfs.Config{FileName: fn, MaxChunkSize: 1024 * 1024}
	c, err := chunkfs.New(context.Background(), cfg, tc.p)
	if err != nil {
		t.Fatal("Could not create new chunk err=", err)
	}
	tc.cks = append(tc.cks, c)
	return c
}

func (tc *tstCtx) Close() {
	for _, c := range tc.cks {
		c.Close()
	}
	tc.p.Close()
	os.RemoveAll(tc.dir) // clean up
}

func (tc *tstCtx) checkGrEqPos(t *testing.T, src string, cid chunk.Id, ts int64, pos int) {
	pos1, err := tc.tidx.GetPosForGreaterOrEqualTime(src, cid, ts)
	if err == ErrOutOfRange && pos == -1 {
		return
	}

	if err != nil {
		t.Fatal("checkGrEqPos(): src=", src, ", cid=", cid, " ts=", ts, " expected pos=", pos, ", but pos1=", pos1, ", err=", err)
	}

	if int(pos1) != pos {
		t.Fatal("checkGrEqPos(): for ts=", ts, " expected pos=", pos, ", but return pos1=", pos1)
	}
}

func (tc *tstCtx) checkLessPos(t *testing.T, src string, cid chunk.Id, ts int64, pos int) {
	pos1, err := tc.tidx.GetPosForLessTime(src, cid, ts)
	if err == ErrOutOfRange && pos == -1 {
		return
	}

	if err != nil {
		t.Fatal("checkLessPos(): src=", src, ", cid=", cid, " ts=", ts, " expected pos=", pos, ", but pos1=", pos1, ", err=", err)
	}

	if int(pos1) != pos {
		t.Fatal("checkLessPos(): expected pos=", pos, ", for ts=", ts, ", but return pos1=", pos1)
	}
}
