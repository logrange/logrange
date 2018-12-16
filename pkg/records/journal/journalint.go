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
	"fmt"
	"sort"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/util"
)

type (
	// ChnksController allows to create and access to the journal's chunks, it is used
	// by the journal.journal implementation.
	ChnksController interface {
		// JournalName returns the jounal name it controls
		JournalName() string

		// GetChunkForWrite returns chunk for write operaion, the call could
		// be cancelled via ctx.
		// ctx == nil is acceptable
		GetChunkForWrite(ctx context.Context) (chunk.Chunk, error)

		// Chunks returns a sorted list of chunks. ctx can cancel the call. ctx
		// could be nil.
		Chunks(ctx context.Context) (chunk.Chunks, error)

		// WaitForNewData waits till new data appears in the journal, or the
		// ctx is closed. Will return an error if any. or indicates the new
		// data is added to the journal
		WaitForNewData(ctx context.Context, pos Pos) error
	}

	journal struct {
		cc     ChnksController
		logger log4g.Logger
	}
)

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

// Write - writes records received from the iterator to the journal.
func (j *journal) Write(ctx context.Context, rit records.Iterator) (int, Pos, error) {
	var err error
	for _, err = rit.Get(ctx); err == nil; {
		c, err := j.cc.GetChunkForWrite(ctx)
		if err != nil {
			return 0, Pos{}, err
		}

		// we either would be able to write something or all to the chunk, or will get
		// an error
		n, offs, err := c.Write(ctx, rit)
		if n > 0 {
			return n, Pos{c.Id(), offs}, err
		}

		if err != util.ErrMaxSizeReached {
			break
		}
	}
	return 0, Pos{}, err
}

// Sync tells write chunk to be synced. Only known application for it is in tests
func (j *journal) Sync() {
	c, _ := j.cc.GetChunkForWrite(nil)
	if c != nil {
		c.Sync()
	}
}

func (j *journal) Iterator() Iterator {
	return &iterator{j: j}
}

func (j *journal) Size() int64 {
	chunks, _ := j.cc.Chunks(nil)
	var sz int64
	for _, c := range chunks {
		sz += c.Size()
	}
	return sz
}

func (j *journal) String() string {
	return fmt.Sprintf("{name=%s}", j.cc.JournalName())
}

// getChunkById is looking for a chunk with cid. If there is no such chunk
// in the list, it will return the chunk with lowest Id, but which is greater
// then the cid. If there is no such chunks, so cid points is out
// of the chunks range, then the method returns nil
func (j *journal) getChunkById(cid chunk.Id) chunk.Chunk {
	chunks, _ := j.cc.Chunks(nil)
	n := len(chunks)
	if n == 0 {
		return nil
	}

	idx := sort.Search(len(chunks), func(i int) bool { return chunks[i].Id() >= cid })
	// according to the condition idx is always in [0..n]
	if idx < n {
		return chunks[idx]
	}
	return nil
}
