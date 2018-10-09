package inmem

import (
	"encoding/binary"
	"io"

	"github.com/logrange/logrange/pkg/records"
)

type (
	// Reader can iterate over Records. It implements data.Iterator interface
	// with accessing to the records in forward direction.
	Reader struct {
		buf  []byte
		cur  []byte
		offs int
		cnt  int
	}
)

// Reset initializes Reader and checks whether the provided buf
// is properly organized. Returns an error if the structure is incorrect.
// If the method returns an error, buffer will be reset to an empty value
func (bbi *Reader) Reset(buf []byte) error {
	_, cnt, err := Check(buf)
	bbi.buf = nil
	bbi.cur = nil
	bbi.offs = 0
	bbi.cnt = cnt

	if err != nil {
		return err
	}

	bbi.buf = buf
	bbi.fillCur()

	return nil
}

func (bbi *Reader) fillCur() {
	if bbi.offs < len(bbi.buf) {
		ln := binary.BigEndian.Uint32(bbi.buf[bbi.offs:])
		if ln != cEofMarker {
			offs := bbi.offs + 4
			bbi.cur = bbi.buf[offs : offs+int(ln)]
			return
		}
	}
	bbi.cur = nil
	bbi.offs = len(bbi.buf)
}

// Get returns current element
func (bbi *Reader) Get() (records.Record, error) {
	if bbi.End() {
		return nil, io.EOF
	}
	return records.Record(bbi.cur), nil
}

// Next switches to the next element. Data() allows to access to the current one.
// Has no effect if the end is reached
func (bbi *Reader) Next() {
	bbi.offs += 4 + len(bbi.cur)
	bbi.fillCur()
}

// Buf returns underlying buffer
func (bbi *Reader) Buf() []byte {
	return bbi.buf
}

// End returns whether the end of the records list is reached.
func (bbi *Reader) End() bool {
	return bbi.offs == len(bbi.buf)
}

// Len returns number of records found in the buf
func (bbi *Reader) Len() int {
	return bbi.cnt
}
