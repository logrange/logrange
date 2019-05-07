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
 tmindex package contains structures and interfaces which allows to build time
 indexes on top of journal chunks. The time indexes allow to identify chunk
 segments where data with a certain time is collected
*/

package tmindex

import (
	"context"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/records/chunk"
)

type (
	// TsIndexer interface defines the public interface for the chunk indexes
	TsIndexer interface {
		// OnWrite notifies the indexer about new data added
		OnWrite(src string, firstRec, lastRec uint32, rInfo RecordsInfo) error

		// LastChunkRecordsInfo returns information about last chunk, or an error if not found
		LastChunkRecordsInfo(src string) (res RecordsInfo, err error)

		// GetPosForGreaterOrEqualTime returns the position in the chunk which records'
		// timestamps are greater or equal to ts starting from the position. Could return
		// the following errors:
		// NotFound - there is no such partition or the chunk Id
		// ErrTmIndexCorrupted - there is a timestamp in the index, but it is corrupted, must be rebuilt, but
		// 				starting from 0, most probably will get some records
		// ErrOutOfRange - all known timestamps are less than ts
		GetPosForGreaterOrEqualTime(src string, cid chunk.Id, ts uint64) (uint32, error)

		// GetPosForLessTime returns the position in the chunk which records' timestamps are
		// less than ts starting from the position. Could return the following errors:
		// NotFound - there is no such partition or the chunk Id
		// ErrTmIndexCorrupted - there is a timestamp in the index, but it is corrupted, must be rebuilt, but
		// 				starting from tail, most probably will get some records
		// ErrOutOfRange - all known timestamps are less than ts
		GetPosForLessTime(src string, cid chunk.Id, ts uint64) (uint32, error)

		// SyncChunks provides list of known chunks and update their information locally
		SyncChunks(ctx context.Context, src string, cks chunk.Chunks) []RecordsInfo

		// Rebuild Index allows to re-build time index for the chunk provided. It
		// runs the procedure for building the time index if it is marked as corrupted
		RebuildIndex(ctx context.Context, src string, ck chunk.Chunk)
	}

	// TsIndexerConfig struct contains settings for the component
	TsIndexerConfig struct {
		// Dir is where the data will be persisted
		Dir string
	}

	// RecordsInfo is used to describe a records range information
	RecordsInfo struct {
		Id    chunk.Id
		MinTs uint64
		MaxTs uint64
	}

	// tmidx struct supports TsIndexer
	tmidx struct {
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

// SyncChunks provides list of known chunks and update their information locally
func (ti *tmidx) SyncChunks(ctx context.Context, src string, cks chunk.Chunks) []RecordsInfo {
	return ti.ci.syncChunks(ctx, src, cks)
}

// RebuildIndex is part of TsIndexer
func (ti *tmidx) RebuildIndex(ctx context.Context, src string, ck chunk.Chunk) {
	ti.ci.rebuildIndex(ctx, src, ck)
}

// GetPosForGreaterOrEqualTime is part of TsIndexer
func (ti *tmidx) GetPosForGreaterOrEqualTime(src string, cid chunk.Id, ts uint64) (uint32, error) {
	return ti.ci.getPosForGreaterOrEqualTime(src, cid, ts)
}

// GetPosForLessTime is part of TsIndexer
func (ti *tmidx) GetPosForLessTime(src string, cid chunk.Id, ts uint64) (uint32, error) {
	return ti.ci.getPosForLessTime(src, cid, ts)
}

func (ri RecordsInfo) String() string {
	return fmt.Sprintf("{Id: %s, MinTs: %d, MaxTs: %d}", ri.Id, ri.MinTs, ri.MaxTs)
}
