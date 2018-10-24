package fs

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logrange/logrange/pkg/records/inmem"
)

var (
	//	ErrMaxSizeReached = errors.New("Maximum size of the object is reached.")
	//	ErrUnableToWrite  = errors.New("The write is not possible")
	ErrWrongOffset = fmt.Errorf("Error wrong offset while seeking")

	//	ErrUnableToRead   = errors.New("Chunk is closed or not ready for read")
	ErrWrongState = fmt.Errorf("Wrong state, probably already closed.")

	ErrBufferTooSmall = fmt.Errorf("Too small buffer for read")
	ErrCorruptedData  = fmt.Errorf("Chunk data has an inconsistency in the data inside")

//	ErrRecordTooBig   = errors.New("Record is too big. Unacceptable.")
//	ErrCancelled      = errors.New("Operation is cancelled")

//	MinRecordId = RecordId{}
//	MaxRecordId = RecordId{ChunkId: math.MaxUint32, Offset: math.MaxInt64}

//	glock sync.Mutex
//	// chunk read descriptor closer keeps counting on opened desriptors
//	chnk_rd_clsr *chunkRdCloser
)

type (
	Config struct {
		FileName string
	}

	Chunk struct {
		fileName string
		// last synced record offset
		lro int64

		lock   sync.Mutex
		frpool *frPool
		closed bool

		// writing part
		w    *fWriter
		wlro int64
	}
)

const (
	ChnkDataHeaderSize  = 4
	ChnkDataRecMetaSize = ChnkDataHeaderSize * 2
	ChnkWriterIdleTO    = time.Minute
	ChnkWriterFlushTO   = 500 * time.Millisecond

	ChnkWriterBufSize = 4 * 4096
	ChnkReaderBufSize = 2 * 4096
)

func New(cfg *Config) (*Chunk, error) {
	return nil, nil
}

// readForward reads records in ascending order.
// It will write to bbw for arranging payloads of the read records. The bbw size should
// be big enough to hold at least one record, otherwise ErrBufferTooSmall is reported
//
// First record for the read can be found by offset, the next records
// will be read in forward, ascending order. The method will try to read records
// until the bbw is full, or maxRecs records if the buffer is extendable
// or too big. It will return number of records it actually read, offset for
// the next record, and an error if any.
//
// offset < 0 will start from beginning. Offset behind the last record will cause io.EOF error
// if at least one record is read error will be nil.
func (c *Chunk) readForward(fr *fReader, offset int64, maxRecs int, bbw *inmem.Writer) (int, int64, error) {
	if offset < 0 {
		offset = 0
	}

	if c.isOffsetBehindLast(offset) {
		return 0, offset, io.EOF
	}

	err := fr.seek(offset)
	if err != nil {
		return 0, offset, err
	}

	var szBuf [ChnkDataHeaderSize]byte
	rdSlice := szBuf[:]
	for i := 0; i < maxRecs; i++ {
		if c.isOffsetBehindLast(offset) {
			return i, offset, nil
		}

		// top size
		_, err = fr.read(rdSlice)
		if err != nil {
			return i, offset, err
		}

		sz := int(binary.BigEndian.Uint32(rdSlice))
		var rb []byte
		rb, err = bbw.Allocate(sz, i == 0)
		if err != nil {
			if i == 0 {
				return i, offset, ErrBufferTooSmall
			}
			return i, offset, nil
		}

		// the record payload
		_, err = fr.read(rb)
		if err != nil {
			bbw.FreeLastAllocation(sz)
			return i, offset, err
		}

		// bottom size
		_, err = fr.read(rdSlice)
		if err != nil {
			bbw.FreeLastAllocation(sz)
			return i, offset, err
		}

		bsz := int(binary.BigEndian.Uint32(rdSlice))
		if sz != bsz {
			return i, offset, ErrCorruptedData
		}
		offset += int64(sz) + ChnkDataRecMetaSize
	}

	return maxRecs, offset, nil
}

func (c *Chunk) isOffsetBehindLast(offset int64) bool {
	return offset > atomic.LoadInt64(&c.lro)
}

func (c *Chunk) Close() error {
	return nil
}
