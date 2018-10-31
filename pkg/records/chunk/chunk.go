package chunk

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"
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
		Write(ctx context.Context, it records.Iterator) (int, int64, error)

		// Iterator returns a chunk.Iterator object to read records from the chunk
		Iterator() (Iterator, error)

		// Size returns the size of the chunk
		Size() int64

		// Returns Last record offset. The value could be negative, what means
		// no records available in the chunk yet.
		GetLastRecordOffset() int64
	}

	Chunks []Chunk

	// Iterator is a records.Iterator extension, which allows to read records
	// from a Сhunk.
	//
	// Chunk suppposes that the records are resided in a storage. Each record
	// has its offset or position. The position could be in the range [-1..Chunk.Size()]
	// Current position contains position of the record which could be read by
	// Get() function. Position could be changed via Next() function or via
	// SetPos() function. Two corner values: -1 (record before first one),
	// or Chunk.Size() (record which will come after last one) are allowed.
	// All other values outside of the range above are prohibited. For position
	// with value=-1, the Get() method always returns (nil, io.EOF), for position
	// value set to Chunk.Size() the Get() function can either return (nil, io.EOF)
	// or a new record, which could be added to the chunk after the position has been set.
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

		// SetBackward allows to specify direction of the iterator. Backward means
		// LIFO, but forward means FIFO order.
		//
		// Changing direction can cause the Get() will return different result
		// without shifting the position by Nest() or SetPos() calls. It could
		// happen when  the iterator positio is either -1, or Chunk.Size(). So
		// if the iterator position was forward
		// and the end is reached (io.EOF is returned), changing the iterator
		// direction to backward with consequtieve Get() call will return the
		// last record in the chunk. Same is true for backward. If the io.EOF
		// was returned and the direction has been changed to forward after that
		// the Get() function will return first record in the chunk.
		SetBackward(bkwd bool)

		// Pos returns the current iterator position within the chunk. It could
		// return value in [-1..Chunk.Size()] range
		Pos() int64

		// SetPos sets the current position within the chunk. The pos param must be in
		// between [-1..ChunkSize]. The pos points to the record offset in the
		// chunk data storage. So, if the pos has incorrect value in [0..ChunkSize)
		// range the iterator Get() function will constantly return an error
		// which will indicate the data storage is inconsistent or the position is
		// wrong.
		//
		// Two corner position values -1 and Chunk.Size() are allowed. Setting
		// pos to -1 will cause that Get() function returns (nil, io.EOF) for
		// backward iterator until the position is changed via SetPos() call.
		//
		// Setting pos to -1 for forward iterator means that it will return
		// first record in the chunk.
		//
		// Setting pos to Chunk.Size() value can cause that the Get() function will
		// return (nil, io.EOF) or the new record, which has been added to the
		// chunk after the SetPos() call.
		//
		// The function returns value which was set by the call.
		SetPos(pos int64) int64
	}

	// Listener an interface which can be used for receiving some chunk
	// events (like LRO changes)
	Listener interface {

		// OnLROChange is called when the LRO is changed
		OnLROChange(c Chunk)
	}
)

var lastCid uint64

// NewCId generates new the host unique chunk id. The chunk IDs are sortable,
// lately created chunks have greater ID values than older ones.
func NewCId() uint64 {
	for {
		cid := (uint64(time.Now().UnixNano()) & 0xFFFFFFFFFFFF0000) | uint64(util.HostId16&0xFFFF)
		lcid := atomic.LoadUint64(&lastCid)
		if lcid >= cid {
			cid = lcid + 0x10000
		}
		if atomic.CompareAndSwapUint64(&lastCid, lcid, cid) {
			return cid
		}
	}
}
