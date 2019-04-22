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
	"crypto/sha1"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/chunk"
)

type (
	// Controller provides an access to known journals
	//
	// Clients should use the interface for accessing to journals and their
	// states
	Controller interface {
		// Visit iterate over all known journals and it calls the cv for every journal found. Iteration is
		// stopped when all journals are visited or cv returns false
		Visit(ctx context.Context, cv ControllerVisitorF)

		// GetOrCreate creates new, or gives an access to existing journal
		GetOrCreate(ctx context.Context, jname string) (Journal, error)

		// Delete removes the internal record, but doesn't touch the journal data.
		// an error could be returned if there are alive chunks registered. The journal could be
		// re-created following up calls by GetOrCreate
		Delete(ctx context.Context, jname string) error
	}

	// ControllerVisitor is a func which will be called by Visit() function (see Controller). It must return
	// false if the process should be stopped
	ControllerVisitorF func(j Journal) bool

	// Pos defines a position within a journal. Can be ordered.
	Pos struct {
		CId chunk.Id
		Idx uint32
	}

	// Journal interface describes a journal
	Journal interface {

		// Name returns the journal name
		Name() string

		// Write - writes records received from the iterator to the journal.
		// It returns number of records written, next record write position and an error if any
		Write(ctx context.Context, rit records.Iterator) (int, Pos, error)

		// Size returns the summarized chunks' size
		Size() uint64

		// Count returns number of records in the journal
		Count() uint64

		// Iterator returns an iterator to walk through the journal records
		Iterator() Iterator

		// Sync could be called after a write to sync the written data with the
		// storage to be sure the read will be able to read the new added
		// data
		Sync()

		// ChnksController returns ChnksController for the journal
		Chunks() ChnksController
	}

	// ChnksController allows to create and access to the journal's chunks, it is used
	// by the journal.journal implementation.
	ChnksController interface {
		// JournalName returns the jounal name it controls
		JournalName() string

		// GetChunkForWrite returns chunk for write operaion, the call could
		// be cancelled via ctx. The excludeCid could contain the chunk.Id which should
		// not be returned. Providing excludeCid the client indicates that it already
		// tries to write to the chunk and the operation was not successful, so it should not
		// be repeated.
		// ctx == nil is acceptable
		GetChunkForWrite(ctx context.Context, excludeCid chunk.Id) (chunk.Chunk, error)

		// Chunks returns a sorted list of chunks. ctx can cancel the call. ctx
		// could be nil.
		Chunks(ctx context.Context) (chunk.Chunks, error)

		// WaitForNewData waits till new data appears in the journal, or the
		// ctx is closed. Will return an error if any. or indicates the new
		// data is added to the journal
		WaitForNewData(ctx context.Context, pos Pos) error

		// DeleteChunk marks the journal's chunk with Id <= lastCid as deleted. The cdf will be called as soon as a chunk
		// will be marked as deleted. The DeleteChunk doesn't delete the chunk data physically, but it gets the chunk out from
		// the service loop. The callback function could either delete the data or archive it.
		// The function returns number of chunks makred as deleted or an error, if any
		DeleteChunks(ctx context.Context, lastCid chunk.Id, cdf OnChunkDeleteF) (int, error)

		// LocalFolder returns the folder on the FS where all the chunks are stored
		LocalFolder() string
	}

	// OnChunkDeleteF is callback function provided to ChnksController.DeleteChunk. It will be invoked as soon as the
	// chunk is marked as deleted.
	OnChunkDeleteF func(cid chunk.Id, filename string, err error)

	// Iterator interface provides a journal iterator
	Iterator interface {
		io.Closer
		records.Iterator

		Pos() Pos

		// SetPos allows to change the iterator position
		SetPos(pos Pos)
	}
)

const JOURNAL_NAME_REGEX = ".+"

var NameRegExp *regexp.Regexp

func init() {
	var err error
	NameRegExp, err = regexp.Compile(JOURNAL_NAME_REGEX)
	if err != nil {
		panic(err)
	}
}

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

func ParsePos(pstr string) (Pos, error) {
	if len(pstr) == 0 {
		return Pos{}, nil
	}

	if len(pstr) != 24 {
		return Pos{}, fmt.Errorf("The \"%s\" doesn't look like a journal position.", pstr)
	}

	ckId, err := strconv.ParseUint(pstr[:16], 16, 64)
	if err != nil {
		return Pos{}, fmt.Errorf("Could not parse chunkId from %s of %s", pstr[:16], pstr)
	}

	pos, err := strconv.ParseUint(pstr[16:], 16, 32)
	if err != nil {
		return Pos{}, fmt.Errorf("Could not parse record index from %s of %s", pstr[16:], pstr)
	}

	return Pos{chunk.Id(ckId), uint32(pos)}, nil
}
