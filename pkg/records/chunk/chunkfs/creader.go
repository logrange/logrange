package chunkfs

import (
	"encoding/binary"
)

type (
	// cReader is a helper internal struct, which allows to read data using a
	// fReader. The fReader doesn't have some states, but just use fReader to
	// support the file structure
	cReader struct {
		dr    *fReader
		ir    *fReader
		szBuf [ChnkDataHeaderSize]byte
	}
)

// readRecord reads one record using the provided slice(buf) for storing the data.
// if the buf size is not big enough for storing the record, it will return
// ErrBufferTooSmall error. The method shifts current position to the next record to be
// read
// it returns the result buffer, which must be next and an error, if any
func (cr *cReader) readRecord(buf []byte) ([]byte, error) {
	rdSlice := cr.szBuf[:]

	// the cur record size
	_, err := cr.dr.read(rdSlice)
	if err != nil {
		return nil, err
	}

	sz := int(binary.BigEndian.Uint32(rdSlice))
	if len(buf) < sz {
		return nil, ErrBufferTooSmall
	}

	// payload
	res := buf[:sz]
	_, err = cr.dr.read(res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// setPos expects the index of the record (starting from 0), which will be
// read next
func (cr *cReader) setPos(pos uint32) error {
	if cr.ir == nil {
		return nil
	}

	err := cr.ir.seek(int64(pos) * ChnkIndexRecSize)
	if err != nil {
		return err
	}

	var offsArr [ChnkIndexRecSize]byte
	offsBuf := offsArr[:]
	_, err = cr.ir.read(offsBuf)
	if err != nil {
		return err
	}

	offs := int64(binary.BigEndian.Uint64(offsBuf))
	return cr.dr.seek(offs)
}
