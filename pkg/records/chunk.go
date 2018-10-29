package records

import (
	"context"
	"fmt"
	"io"
)

type (
	// Chunk is an abstraction which represents a records storage. It has its own
	// id, which should be sortable within a group of chunks like a journal.
	Chunk interface {
		// Chunk inherits io.Closer interface
		io.Closer

		// Id returns the chunk id which must be unique between a group of
		// chunks like a journal. Also it is sortable, so chunks with less
		// value of the id comes before the chunks with greater values
		Id() uint64

		// Write allows to write records to the chunk. It expects contxt and
		// the iterator, which provides the records source.
		//
		// Write returns 3 values:
		// 1. int value contains the number of records written
		// 2. int64 value which contains the last written record offset
		// 3. an error if any:
		// 		ErrMaxSizeReached - when the write cannot be done because of
		//				the size limits
		Write(ctx context.Context, it Iterator) (int, int64, error)

		// Iterator returns a ChunkIterator object to read records from the chunk
		Iterator() (ChunkIterator, error)

		// Size returns the size of the chunk
		Size() int64
	}

	// ChunkIterator is an Iterator (see) extension, which allows to read records
	// from the chunk the iterator is associated with.
	ChunkIterator interface {
		// ChunkIterator ihnerits io.Closer interfase. Being called the behavior
		// of the iterator is unpredictable and it must not be used anymore
		io.Closer
		// ChunkIterator inherits Iterator interface
		Iterator

		// Release is an implementation specific function which allows to
		// release underlying resources. It can be called by the iterator using
		// code to let the implementation know that underlying resources can
		// be freed. There is no guarantee that the iterator will be used after
		// the call again. Implementation should guarantee that it will behave
		// same way after the call as if it never called.
		Release()

		// GetPos returns the current iterator position within the chunk
		GetPos() int64
		// SetPos sets the current position within the chunk
		SetPos(pos int64)
	}
)

var (
	ErrMaxSizeReached = fmt.Errorf("Could not perform the write operation. The maximum size of the chunk is reached.")
)
