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
	"github.com/logrange/logrange/pkg/tmindex"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/journal"
	"github.com/logrange/range/pkg/utils/errors"
)

// tstJournal
type tstJournal struct {
	name string
	cc   *tstChnksController
}

func (tj *tstJournal) Name() string { return tj.name }
func (tj *tstJournal) Write(ctx context.Context, rit records.Iterator) (int, journal.Pos, error) {
	return 0, journal.Pos{}, nil
}
func (tj *tstJournal) Size() uint64                    { return 0 }
func (tj *tstJournal) Count() uint64                   { return 0 }
func (tj *tstJournal) Iterator() journal.Iterator      { return nil }
func (tj *tstJournal) Sync()                           {}
func (tj *tstJournal) Chunks() journal.ChnksController { return tj.cc }

// tstChnksController
type tstChnksController struct {
	cks chunk.Chunks
}

func (tcc *tstChnksController) JournalName() string { return "" }
func (tcc *tstChnksController) GetChunkForWrite(ctx context.Context, excludeCid chunk.Id) (chunk.Chunk, error) {
	return nil, nil
}
func (tcc *tstChnksController) Chunks(ctx context.Context) (chunk.Chunks, error)          { return tcc.cks, nil }
func (tcc *tstChnksController) WaitForNewData(ctx context.Context, pos journal.Pos) error { return nil }
func (tcc *tstChnksController) DeleteChunks(ctx context.Context, lastCid chunk.Id, cdf journal.OnChunkDeleteF) (int, error) {
	return 0, nil
}
func (tcc *tstChnksController) LocalFolder() string { return "" }

// tstChunk
type tstChunk struct {
	id  chunk.Id
	ci  chunk.Iterator
	cnt uint32
}

func (tc *tstChunk) Close() error { return nil }
func (tc *tstChunk) Id() chunk.Id { return tc.id }
func (tc *tstChunk) Write(ctx context.Context, it records.Iterator) (int, uint32, error) {
	return 0, 0, nil
}
func (tc *tstChunk) Sync()                             {}
func (tc *tstChunk) Iterator() (chunk.Iterator, error) { return tc.ci, nil }
func (tc *tstChunk) Size() int64                       { return 0 }
func (tc *tstChunk) Count() uint32                     { return tc.cnt }
func (tc *tstChunk) AddListener(lstnr chunk.Listener)  {}

// tstTsIndexer
type tstTsIndexer struct {
	ris     []tmindex.RecordsInfo
	grEqMap map[chunk.Id]uint32
	lessMap map[chunk.Id]uint32
}

func (tti *tstTsIndexer) OnWrite(src string, firstRec, lastRec uint32, rInfo tmindex.RecordsInfo) error {
	return nil
}
func (tti *tstTsIndexer) LastChunkRecordsInfo(src string) (res tmindex.RecordsInfo, err error) {
	return tti.ris[len(tti.ris)-1], nil
}
func (tti *tstTsIndexer) GetRecordsInfo(src string, cid chunk.Id) (res tmindex.RecordsInfo, err error) {
	for _, ri := range tti.ris {
		if ri.Id == cid {
			return ri, nil
		}
	}
	return tmindex.RecordsInfo{}, errors.NotFound
}
func (tti *tstTsIndexer) GetPosForGreaterOrEqualTime(src string, cid chunk.Id, ts int64) (uint32, error) {
	res, ok := tti.grEqMap[cid]
	if !ok {
		panic("unexpected call for cid=" + cid.String())
	}
	return res, nil
}
func (tti *tstTsIndexer) GetPosForLessTime(src string, cid chunk.Id, ts int64) (uint32, error) {
	res, ok := tti.lessMap[cid]
	if !ok {
		panic("unexpected call for cid=" + cid.String())
	}
	return res, nil
}
func (tti *tstTsIndexer) SyncChunks(ctx context.Context, src string, cks chunk.Chunks) []tmindex.RecordsInfo {
	return tti.ris
}

func (tti *tstTsIndexer) ReadData(src string, cid chunk.Id) ([]tmindex.IdxRecord, error) {
	return nil, nil
}
func (tti *tstTsIndexer) Count(src string, cid chunk.Id) (int, error)                              { return 0, nil }
func (tti *tstTsIndexer) RebuildIndex(ctx context.Context, src string, ck chunk.Chunk, force bool) {}

func (tti *tstTsIndexer) VisitSources(visitor func(src string) bool) {}
