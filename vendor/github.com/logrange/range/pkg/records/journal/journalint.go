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
	"github.com/logrange/range/pkg/records"
	"sort"

	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/utils/errors"
)

type (
	journal struct {
		cc     ChnksController
		logger log4g.Logger
	}
)

// New creates a new journal
func New(cc ChnksController) Journal {
	j := new(journal)
	j.cc = cc
	j.logger = log4g.GetLogger("journal").WithId("{" + j.cc.JournalName() + "}").(log4g.Logger)
	j.logger.Info("New instance created ", j)
	return j
}

// Name returns the name of the journal
func (j *journal) Name() string {
	return j.cc.JournalName()
}

// Write - writes records received from the iterator to the journal. The Write can write only a portion from
// the it. It can happen when max chunk size is hit.
func (j *journal) Write(ctx context.Context, rit records.Iterator) (int, Pos, error) {
	var err error
	var c chunk.Chunk
	var excludeCid chunk.Id
	var n int
	var offs uint32
	for err == nil {
		c, err = j.cc.GetChunkForWrite(ctx, excludeCid)
		if err != nil {
			return 0, Pos{}, err
		}

		// If c.Write has an error, it will return n and offs actually written. So if n > 0 let's
		// consider the operation successful
		n, offs, err = c.Write(ctx, rit)
		if n > 0 {
			return n, Pos{c.Id(), offs}, nil
		}

		if err != errors.MaxSizeReached {
			break
		}

		if c.Id() == excludeCid {
			j.logger.Error("Ooops, same chunk id=", excludeCid, " was returned twice. Stopping the Write operation with the err=", err)
			break
		}
		excludeCid = c.Id()
		err = nil
	}
	return 0, Pos{}, err
}

// Sync tells write chunk to be synced. Only known application for it is in tests
func (j *journal) Sync() {
	c, _ := j.cc.GetChunkForWrite(context.Background(), 0)
	if c != nil {
		c.Sync()
	}
}

func (j *journal) Iterator() Iterator {
	return &iterator{j: j}
}

func (j *journal) Size() uint64 {
	chunks, _ := j.cc.Chunks(nil)
	var sz int64
	for _, c := range chunks {
		sz += c.Size()
	}
	return uint64(sz)
}

func (j *journal) Count() uint64 {
	chunks, _ := j.cc.Chunks(nil)
	var cnt uint64
	for _, c := range chunks {
		cnt += uint64(c.Count())
	}
	return cnt
}

func (j *journal) String() string {
	return fmt.Sprintf("{name=%s}", j.cc.JournalName())
}

func (j *journal) getChunkByIdOrGreater(cid chunk.Id) chunk.Chunk {
	chunks, _ := j.cc.Chunks(context.Background())
	n := len(chunks)
	if n == 0 {
		return nil
	}

	idx := sort.Search(n, func(i int) bool { return chunks[i].Id() >= cid })
	// according to the condition idx is always in [0..n]
	if idx < n {
		return chunks[idx]
	}
	return chunks[n-1]
}

func (j *journal) getChunkByIdOrLess(cid chunk.Id) chunk.Chunk {
	chunks, _ := j.cc.Chunks(context.Background())
	n := len(chunks)
	if n == 0 || chunks[0].Id() > cid {
		return nil
	}

	idx := sort.Search(n, func(i int) bool { return chunks[i].Id() > cid })
	return chunks[idx-1]
}

// Chunks please see journal.Journal interface
func (j *journal) Chunks() ChnksController {
	return j.cc
}
