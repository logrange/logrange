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

/*
 tmindex package contains structures and interfaces which allow to build time
 indexes on top of journal chunks. The time indexes allow to identify chunk
 segments where data with a certain time is collected.
*/

package tmindex

import (
	"context"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/records/chunk"
)

type (
	// TsIndexer interface defines the public interface for the chunk's time indexes
	TsIndexer interface {
		// OnWrite notifies the indexer about new data added. It will create the new time
		// index for the chunk, if it doesn't exist yet. The function returns error if the
		// operation is unsuccessful. It must return ErrTmIndexCorrupted if the index should
		// be rebuilt. This case caller can RebuildIndex() to fix the index.
		OnWrite(src string, firstRec, lastRec uint32, rInfo RecordsInfo) error

		// LastChunkRecordsInfo returns information about last chunk, or an error
		// if it is not found (errors2.NotFound)
		LastChunkRecordsInfo(src string) (res RecordsInfo, err error)

		// GetRecordsInfo returns records info by chunk Id, or an error if the chunk is
		// not found (errors2.NotFound)
		GetRecordsInfo(src string, cid chunk.Id) (res RecordsInfo, err error)

		// Count returns number of records in the time index or it returns error if
		// the index is corrupted (ErrTmIndexCorrupted), or it doesn't exist (errors2.NotFound)
		Count(src string, cid chunk.Id) (int, error)

		// ReadData allows to read all points and return them. The function returns
		// err == errors2.NotFound if either src or the cid is not found. It returns
		// ErrTmIndexCorrupted if the index exists, but it is corrupted
		ReadData(src string, cid chunk.Id) ([]IdxRecord, error)

		// GetPosForGreaterOrEqualTime returns the position in the chunk which records'
		// timestamps are greater or equal to ts starting from the position. Could return
		// the following errors:
		// NotFound - there is no such partition or the chunk Id
		// ErrTmIndexCorrupted - there is a timestamp in the index, but it is corrupted, must be rebuilt, but
		// 				starting from 0, most probably will get some records
		// ErrOutOfRange - all known timestamps are less than ts e.g. maxTime < ts
		GetPosForGreaterOrEqualTime(src string, cid chunk.Id, ts int64) (uint32, error)

		// GetPosForLessTime returns the position in the chunk which records' timestamps are
		// less than ts starting from the position. Could return the following errors:
		// NotFound - there is no such partition or the chunk Id
		// ErrTmIndexCorrupted - there is a timestamp in the index, but it is corrupted, must be rebuilt, but
		// 				starting from tail, most probably will get some records
		// ErrOutOfRange - all known timestamps are greater than ts e.g. minTime > ts
		GetPosForLessTime(src string, cid chunk.Id, ts int64) (uint32, error)

		// SyncChunks expects the list of known chunks in cks. It updates internal chunks information and
		// returns the updated info as a slice of RecordsInfo. The function will remove all
		// chunks and its indexes that are not in the cks. It will remove the source information
		// completely if len(cks) == 0.
		SyncChunks(ctx context.Context, src string, cks chunk.Chunks) []RecordsInfo

		// Rebuild Index allows to re-build time index for the chunk provided. It
		// runs the procedure for building the time index if it is marked as corrupted.
		RebuildIndex(ctx context.Context, src string, ck chunk.Chunk, force bool)

		// VisitSources allows to iterate over the collection of known sources and pass them to the vistior.
		// if the visitor returns false, the function will stop the iteration and get complete the iteration
		VisitSources(visitor func(src string) bool)
	}

	// TsIndexerConfig struct contains settings for the component
	TsIndexerConfig struct {
		// Dir is where the data will be persisted
		Dir string
	}

	// RecordsInfo is used to describe a records range information
	RecordsInfo struct {
		Id    chunk.Id
		MinTs int64
		MaxTs int64
	}

	// IdxRecord a record in the time index
	IdxRecord struct {
		Ts  int64
		Val uint32
	}

	// tmidx struct supports TsIndexer
	tmidx struct {
		TsIndexer

		Config *TsIndexerConfig `inject:""`

		logger log4g.Logger
		ci     *cindex
	}
)

var (
	ErrTmIndexCorrupted = fmt.Errorf("time index corrupted or does not exist.")
	ErrOutOfRange       = fmt.Errorf("the position is out of range of known values")
)

func NewTsIndexer() TsIndexer {
	ti := new(tmidx)
	ti.logger = log4g.GetLogger("tmindex")
	return ti
}

// Init is part of linker.Initializer interface
func (ti *tmidx) Init(ctx context.Context) error {
	ti.ci = newCIndex()
	return ti.ci.init(ti.Config.Dir)
}

// Shutdown is part of linker.Shutdowner interface
func (ti *tmidx) Shutdown() {
	ti.ci.close()
}

// OnWrite notifies the indexer about new data added
func (ti *tmidx) OnWrite(src string, firstRec, lastRec uint32, rInfo RecordsInfo) error {
	return ti.ci.onWrite(src, firstRec, lastRec, rInfo)
}

// LastChunkRecordsInfo returns information about last chunk, or an error if not found
func (ti *tmidx) LastChunkRecordsInfo(src string) (res RecordsInfo, err error) {
	return ti.ci.lastChunkRecordsInfo(src)
}

func (ti *tmidx) GetRecordsInfo(src string, cid chunk.Id) (res RecordsInfo, err error) {
	return ti.ci.getRecordsInfo(src, cid)
}

// SyncChunks provides list of known chunks and update their information locally
func (ti *tmidx) SyncChunks(ctx context.Context, src string, cks chunk.Chunks) []RecordsInfo {
	return ti.ci.syncChunks(ctx, src, cks)
}

// RebuildIndex is part of TsIndexer
func (ti *tmidx) RebuildIndex(ctx context.Context, src string, ck chunk.Chunk, force bool) {
	ti.ci.rebuildIndex(ctx, src, ck, force)
}

// GetPosForGreaterOrEqualTime is part of TsIndexer
func (ti *tmidx) GetPosForGreaterOrEqualTime(src string, cid chunk.Id, ts int64) (uint32, error) {
	return ti.ci.getPosForGreaterOrEqualTime(src, cid, ts)
}

// GetPosForLessTime is part of TsIndexer
func (ti *tmidx) GetPosForLessTime(src string, cid chunk.Id, ts int64) (uint32, error) {
	return ti.ci.getPosForLessTime(src, cid, ts)
}

func (ti *tmidx) ReadData(src string, cid chunk.Id) ([]IdxRecord, error) {
	return ti.ci.readData(src, cid)
}

func (ti *tmidx) Count(src string, cid chunk.Id) (int, error) {
	return ti.ci.count(src, cid)
}

func (ti *tmidx) VisitSources(visitor func(src string) bool) {
	ti.ci.visitSources(visitor)
}

func (ri *RecordsInfo) ApplyTs(ts int64) {
	if ri.MaxTs < ts {
		ri.MaxTs = ts
	}

	if ri.MinTs > ts {
		ri.MinTs = ts
	}
}

func (ri RecordsInfo) String() string {
	return fmt.Sprintf("{Id: %s, MinTs: %d, MaxTs: %d}", ri.Id, ri.MinTs, ri.MaxTs)
}
