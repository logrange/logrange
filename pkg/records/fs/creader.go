package fs

import (
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/logrange/logrange/pkg/records/inmem"
)

type (
	// cReader is a helper internal struct, which allows to read data using a
	// fReader. The fReader doesn't have some states, but just use fReader to
	// support the file structure
	cReader struct {
		fr  *fReader
		lro *int64
	}
)

// readForward - reads records in ascending order.
// It will write results to bbw for arranging payloads of the read records.
// The bbw size should be big enough to hold at least one record, otherwise
// ErrBufferTooSmall is reported.
//
// First record for the read can be found by offset, the next records
// will be read in forward, ascending order. The method will try to read records
// until the bbw is full, or maxRecs records are read. The function will return
// number of records it actually read, offset for the next record, and an error if any.
//
// offset < 0 will start from beginning. Offset behind the last record will cause io.EOF error
// if at least one record is read error will be nil.
func (cr *cReader) readForward(offset int64, maxRecs int, bbw *inmem.Writer) (int, int64, error) {
	if offset < 0 {
		offset = 0
	}

	if cr.isOffsetBehindLast(offset) {
		return 0, offset, io.EOF
	}

	err := cr.fr.seek(offset)
	if err != nil {
		return 0, offset, err
	}

	var szBuf [ChnkDataHeaderSize]byte
	rdSlice := szBuf[:]
	for i := 0; i < maxRecs; i++ {
		if cr.isOffsetBehindLast(offset) {
			return i, offset, nil
		}

		// top size
		_, err = cr.fr.read(rdSlice)
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
		_, err = cr.fr.read(rb)
		if err != nil {
			bbw.FreeLastAllocation(sz)
			return i, offset, err
		}

		// bottom size
		_, err = cr.fr.read(rdSlice)
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

// readBack reads records in descending order.
// It will write records payloads to bbw. The bbw size should
// be big enough to hold at least one record, otherwise ErrBufferTooSmall
// will be returned
//
// First record for the read can be found by offset, the next records
// will be read in descending order. The method will try to read up to
// maxRecs records or when bbw is full, what happens first. It will
// return number of records it actually read, offset for the next one to be read,
// and an error if any.
//
// for negative offsets io.EOF is reported. Offset behind the last record will adjust
// the read to last record. If the chunk is empty, io.EOF will be reported
func (cr *cReader) readBack(offset int64, maxRecs int, bbw *inmem.Writer) (int, int64, error) {
	// adjusting offset for first read
	lro := atomic.LoadInt64(cr.lro)
	if lro < offset {
		offset = lro
	}
	if offset < 0 {
		return 0, -1, io.EOF
	}

	// next record size
	nsz := 0
	var szBuf [ChnkDataHeaderSize]byte
	rdSlice := szBuf[:]
	for i := 0; i < maxRecs; i++ {
		if offset == 0 {
			n, _, err := cr.readForward(offset, 1, bbw)
			return n + i, -1, err
		}

		seekOffs := offset - ChnkDataHeaderSize
		err := cr.fr.smartSeek(seekOffs, nsz+ChnkDataHeaderSize)
		if err != nil {
			return i, offset, err
		}

		// read prev (which is next to be read) record size
		_, err = cr.fr.read(rdSlice)
		if err != nil {
			return i, offset, err
		}
		nsz = int(binary.BigEndian.Uint32(rdSlice)) + int(ChnkDataRecMetaSize)

		// read the current top rec size
		_, err = cr.fr.read(rdSlice)
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

		_, err = cr.fr.read(rb)
		if err != nil {
			return i, offset, err
		}

		// read bottom size
		_, err = cr.fr.read(rdSlice)
		if err != nil {
			return i, offset, err
		}
		bsz := int(binary.BigEndian.Uint32(rdSlice))
		if sz != bsz {
			return i, offset, ErrCorruptedData
		}

		offset -= int64(nsz)
	}

	return maxRecs, offset, nil
}

func (cr *cReader) isOffsetBehindLast(offset int64) bool {
	return offset > atomic.LoadInt64(cr.lro)
}
