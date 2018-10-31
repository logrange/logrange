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
		fr    *fReader
		lro   *int64
		szBuf [ChnkDataHeaderSize]byte
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

	rdSlice := cr.szBuf[:]
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

// readRecord reads one record using the provided slice(buf) for storing the data.
// if the buf size is not big enough for storing the record, it will return
// ErrBufferTooSmall error. If the top size is not equal to bottom one, it will return
// ErrCorruptedData. The method shifts current position to the next record to be
// read
// it returns the result buffer, position, which must be next and an error, if any
func (cr *cReader) readRecord(buf []byte) ([]byte, int64, error) {
	rdSlice := cr.szBuf[:]

	// top size
	_, err := cr.fr.read(rdSlice)
	if err != nil {
		return nil, -1, err
	}

	sz := int(binary.BigEndian.Uint32(rdSlice))
	if len(buf) < sz {
		return nil, -1, ErrBufferTooSmall
	}

	// payload
	_, err = cr.fr.read(buf[:sz])
	if err != nil {
		return nil, -1, err
	}

	// bottom size
	_, err = cr.fr.read(rdSlice)
	if err != nil {
		return nil, -1, err
	}

	bsz := int(binary.BigEndian.Uint32(rdSlice))
	if sz != bsz {
		return nil, -1, ErrCorruptedData
	}

	return buf[:sz], cr.getPos(), nil
}

// readRecordBack read record and shifts the position to previous record.
// it returns the data, offset for the previous position to be read and
// an error, if any.
func (cr *cReader) readRecordBack(buf []byte) ([]byte, int64, error) {
	pos := cr.getPos()
	rdSlice := cr.szBuf[:]
	poffs := int64(-1)
	psz := 0

	// top size current
	_, err := cr.fr.read(rdSlice)
	if err != nil {
		return nil, -1, err
	}

	sz := int(binary.BigEndian.Uint32(rdSlice))
	if len(buf) < sz {
		return nil, -1, ErrBufferTooSmall
	}

	if pos > 0 {
		err := cr.fr.smartSeek(pos-ChnkDataHeaderSize, sz+ChnkDataHeaderSize+ChnkDataRecMetaSize)
		if err != nil {
			return nil, -1, err
		}

		cr.fr.read(rdSlice) // prev record size
		psz = int(binary.BigEndian.Uint32(rdSlice))
		poffs = pos - int64(psz+ChnkDataRecMetaSize)
		cr.fr.read(rdSlice) // cur record size once again
	}

	// payload
	_, err = cr.fr.read(buf[:sz])
	if err != nil {
		return nil, -1, err
	}

	// bottom size
	_, err = cr.fr.read(rdSlice)
	if err != nil {
		return nil, -1, err
	}

	bsz := int(binary.BigEndian.Uint32(rdSlice))
	if sz != bsz {
		return nil, -1, ErrCorruptedData
	}

	if poffs >= 0 {
		err := cr.fr.smartSeek(poffs, psz+ChnkDataRecMetaSize)
		if err != nil {
			return nil, -1, err
		}
	}

	return buf[:sz], poffs, nil
}

func (cr *cReader) getPos() int64 {
	return cr.fr.getNextReadPos()
}

func (cr *cReader) isOffsetBehindLast(offset int64) bool {
	return offset > atomic.LoadInt64(cr.lro)
}
