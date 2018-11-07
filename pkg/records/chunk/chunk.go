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

package chunk

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"
)

type (
	// Id represents a chunk identifier. The NewId() function generates new
	// unique Id.
	//
	// The identifier is 64bit unsigned integer value, which consists of 2 parts.
	// Highest part of the value contains UNIX timestamp in nanoseconds cut to
	// 48bits, and the lowest 16bits contain a process-related  hash which
	// supposed to be unique per a process. Based on the nature of the Id
	// generation, it is supposed that every chunk has it's own unique id and
	// it can be ordered comparing to other chunks in the system.
	Id uint64

	// Chunk is an abstraction which represents a records storage. It has its own
	// id, which should be sortable within a group of chunks like a journal.
	//
	// Chunk cannot contain more than 2^32 records.
	Chunk interface {
		// Chunk inherits io.Closer interface
		io.Closer

		// Id returns the chunk id which must be unique between a group of
		// chunks like a journal. Also it is sortable, so chunks with less
		// value of the id comes before the chunks with greater values
		Id() Id

		// Write allows to write records to the chunk. It expects context and
		// the iterator, which provides the records source.
		//
		// Write returns 3 values:
		// 1. the number of records written (int value)
		// 2. the number of records in the chunk after the write (uint32). The
		// 	number can be greater than value returned by Count, what indicates
		//  that not all written records are persisted yet.
		// 3. an error if any:
		// 		ErrMaxSizeReached - when the write cannot be done because of
		//				the size limits
		Write(ctx context.Context, it records.Iterator) (int, uint32, error)

		// Iterator returns a chunk.Iterator object to read records from the chunk
		Iterator() (Iterator, error)

		// Size returns the size of the chunk
		Size() int64

		// Count returns number of records in the chunk. The number can be
		// less than the Write() function returned value, cause the Count() returns
		// then number of records that can be read, but Write returns the number of
		// records written, but not confirmed to be read yet in the chunk
		Count() uint32
	}

	Chunks []Chunk

	// Iterator is a records.Iterator extension, which allows to read records
	// from a Сhunk.
	//
	// Chunk suppposes that the records are resided in a storage. Each record
	// has its offset or position. The position could be in the range [0..Chunk.Count())
	// Current position contains position of the record which could be read by
	// Get() function. Position could be changed via Next() function or via
	// SetPos() function.
	Iterator interface {
		// The Iterator ihnerits io.Closer interfase. Being called the behavior
		// of the iterator is unpredictable and it must not be used anymore
		io.Closer

		// The Iterator inherits Iterator interface
		records.Iterator

		// Release is an implementation specific function which allows to
		// release underlying resources. It can be called by the iterator using
		// code to let the implementation know that underlying resources can
		// be freed. There is no guarantee that the iterator will be used after
		// the call again. Implementation should guarantee that it will behave
		// same way after the call as if it is never called.
		Release()

		// Pos returns the current iterator position within the chunk. It could
		// return value in [0..Chunk.Count()) range
		Pos() uint32

		// SetPos sets the current position within the chunk. The pos param must be in
		// between [0..Chunk.Count()].
		//
		// Setting pos to Chunk.Count() value, causes that the Get() function will
		// return (nil, io.EOF) or the new record, which has been added to the
		// chunk after the SetPos() call, but before the Get() call.
		SetPos(pos uint32) error
	}

	// Listener an interface which can be used for receiving some chunk
	// events (like OnNewData() notifications)
	Listener interface {

		// OnNewData is called when new records were added to the end
		OnNewData(c Chunk)
	}
)

var lastCid uint64

// NewId generates new the host unique chunk id. The chunk IDs are sortable,
// lately created chunks have greater ID values than older ones.
func NewId() Id {
	for {
		cid := (uint64(time.Now().UnixNano()) & 0xFFFFFFFFFFFF0000) | uint64(util.HostId16&0xFFFF)
		lcid := atomic.LoadUint64(&lastCid)
		if lcid >= cid {
			cid = lcid + 0x10000
		}
		if atomic.CompareAndSwapUint64(&lastCid, lcid, cid) {
			return Id(cid)
		}
	}
}

// ParseCId receives cid - string representation of an Id and tries to turn it
// to Id. Returns an error if the string cannot be decoded.
func ParseId(cid string) (Id, error) {
	c, err := strconv.ParseUint(cid, 16, 64)
	return Id(c), err
}

// String returns a string representation of the chunk Id
func (id Id) String() string {
	return fmt.Sprintf("%016X", uint64(id))
}
