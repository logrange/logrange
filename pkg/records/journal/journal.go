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
	"crypto/sha1"
	"fmt"
	"io"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	// Controller provides an access to known journals
	Controller interface {
		GetOrCreate(ctx context.Context, jname string) (Journal, error)
	}

	// ChnksController allows to create and manage a journal's chunks
	ChnksController interface {
		// GetChunks returns known chunks for the journal sorted by their
		// chunk IDs. The function returns non-nil Chunks slice, which
		// always has at least one chunk. If the journal has just been
		// created the ChnksController will create new chunk for it.
		//
		// The resulted collection can be stored or used for iteration so as
		// it is immutable
		GetChunks(j Journal) chunk.Chunks

		// NewChunk creates new chunk for the provided journal and returns
		// the immutable chunk.Chunks sorted slice
		NewChunk(ctx context.Context, j Journal) (chunk.Chunks, error)
	}

	// Pos defines a position within a journal. Can be ordered.
	Pos struct {
		CId chunk.Id
		Idx uint32
	}

	// Journal interface describes a journal
	Journal interface {
		io.Closer

		// Name returns the journal name
		Name() string

		// Write - writes records received from the iterator to the journal.
		// It returns number of records written, first record position and an error if any
		Write(ctx context.Context, rit records.Iterator) (int, Pos, error)

		// Size returns the summarized chunks size
		Size() int64

		// Iterator returns an iterator to walk through the journal records
		Iterator() (Iterator, error)
	}

	// Iterator interface provides a journal iterator
	Iterator interface {
		io.Closer
		records.Iterator

		Pos() Pos

		// SetPos allows to change the iterator position
		SetPos(pos Pos)
	}
)

// JidFromName returns a journal id (jid) by its name
func JHashFromName(jname string) uint64 {
	ra := sha1.Sum(records.StringToByteArray(jname))
	return (uint64(ra[7]) << 56) | (uint64(ra[6]) << 48) | (uint64(ra[5]) << 40) |
		(uint64(ra[4]) << 32) | (uint64(ra[3]) << 24) | (uint64(ra[2]) << 16) |
		(uint64(ra[1]) << 8) | uint64(ra[0])
}

func (jp Pos) String() string {
	return fmt.Sprintf("%016X%08X", uint64(jp.CId), jp.Idx)
}

func (jp Pos) Less(jp2 Pos) bool {
	return jp.CId < jp2.CId || (jp.CId == jp2.CId && jp.Idx < jp2.Idx)
}
