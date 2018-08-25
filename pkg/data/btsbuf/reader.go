// btsbuf contains functions and structures for working with BtsBuf. The
// BtsBuf is a specially formatted slice of bytes. The format contains size of
// record following by the data. If the size of next record is 0xFFFFFFFF, which
// is named EofMarker, then it means no records in the slice anymore:
//
// +--------------+-----------------+--------------+--  -+----------+-----+
// | 1st rec sizd | 1st record data | 2nd rec size | ... |0xFFFFFFFF|.... |
// +--------------+-----------------+--------------+--  -+----------+-----+
//
// BtsBuf can be formed without EofMaker if the last record is ended at the
// end of the slice:
//
// +--------------+-----------------+-- --+--------------+------------------+
// | 1st rec sizd | 1st record data | ... |last rec size | last record data |
// +--------------+-----------------+-- --+--------------+------------------+
//
package btsbuf

import (
	"encoding/binary"
	"fmt"
	"io"
)

type (
	// Reader can iterate over slice of bytes which contains some
	// chunks of bytes and return them. It implements data.Iterator interface
	// with accessing to the records in forward direction.
	Reader struct {
		buf  []byte
		cur  []byte
		offs int
		cnt  int
	}
)

const (
	cEofMarker = uint32(0xFFFFFFFF)
)

// Reset initializes Reader and checks whether the provided buf
// is properly organized. Returns an error if the structure is incorrect.
// If the method returns an error
func (bbi *Reader) Reset(buf []byte) error {
	cnt, err := check(buf)
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
func (bbi *Reader) Get() ([]byte, error) {
	if bbi.End() {
		return nil, io.EOF
	}
	return bbi.cur, nil
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

func (bbi *Reader) End() bool {
	return bbi.offs == len(bbi.buf)
}

// Len returns number of records found in the buf
func (bbi *Reader) Len() int {
	return bbi.cnt
}

// check will make a check if the buf is properly organized and iteratable.
// The function returns number of records found and an error, if any
func check(buf []byte) (int, error) {
	cnt := 0
	offs := 0
	for offs < len(buf) {
		if offs > len(buf)-4 {
			return 0, fmt.Errorf("Broken BtsBuf structure: on the record %d has offset %d, which points out of the buffer bounds len=%d", cnt, offs, len(buf))
		}

		ln := binary.BigEndian.Uint32(buf[offs:])
		if ln == cEofMarker {
			// ok, got a end buffer marker, so consider it is over
			offs = len(buf)
			break
		}

		cnt++
		oldOffs := offs
		offs += int(ln) + 4
		if oldOffs >= offs {
			return 0, fmt.Errorf("Broken BtsBuf structure: has wrong offset for record %d, its offset %d is less than for previoud record(%d)", cnt, offs, oldOffs)
		}
	}

	if offs == len(buf) {
		return cnt, nil
	}

	return 0, fmt.Errorf("Broken BtsBuf structure: offset=%d for record %d, is out of the buffer bounds(size=%d)", offs, cnt, len(buf))
}
