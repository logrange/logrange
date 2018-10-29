// inmem package contains functions and structures for working with records (slices of bytes)
// stored in the memory in a Records type variable.
package inmem

import (
	"encoding/binary"
	"fmt"
)

type (
	// Records is a slice of bytes which has a special format. Every record in
	// the Records buffer is represented by 2 fields - the record size and its
	// data. The data field could be skipped, if its size is 0 or the end of
	// records marker (EofMarker) is met. The EofMarker is the size field which
	// has 0xFFFFFFFF value:
	//
	// +--------------+-----------------+--------------+--  -+----------+-----+
	// | 1st rec sizd | 1st record data | 2nd rec size | ... |0xFFFFFFFF|.... |
	// +--------------+-----------------+--------------+--  -+----------+-----+
	//
	// Records buffer could be built without EofMaker if the last record is ended at the
	// end of the slice (which is defined by its LENGTH, but NOT A CAPACITY):
	//
	// +--------------+-----------------+-- --+--------------+------------------+
	// | 1st rec sizd | 1st record data | ... |last rec size | last record data |
	// +--------------+-----------------+-- --+--------------+------------------+
	//
	Records []byte
)

const (
	cEofMarker = uint32(0xFFFFFFFF)
)

// Check receives a slice of bytes and checks whether it is formatted as Records
// slice or not. The function returns Records object, number of records found
// and an error if any. If an error is returned, the Records and number of
// records values don't make a sense.
func Check(buf []byte) (Records, int, error) {
	cnt := 0
	offs := 0
	for offs < len(buf) {
		if offs > len(buf)-4 {
			return nil, 0, fmt.Errorf("Invalid buffer format: At least 4 bytes for the size field of the record #%d within the buffer range 0..%d is expected, but the offset=%d", cnt, len(buf), offs)
		}

		sz := binary.BigEndian.Uint32(buf[offs:])
		if sz == cEofMarker {
			// ok, got EofMarker, so consider data is over
			offs = len(buf)
			break
		}

		cnt++
		oldOffs := offs
		offs += int(sz) + 4
		if oldOffs >= offs {
			return nil, 0, fmt.Errorf("Invalid buffer format: Invalid next record offset=%d. It seems the record #%d size=%d overflows its maximum value, because it is expected to be at least %d", offs, cnt, sz, oldOffs)
		}
	}

	if offs == len(buf) {
		return Records(buf), cnt, nil
	}

	return nil, 0, fmt.Errorf("Invalid buffer format: offset=%d for record %d, is out of the buffer bounds(size=%d)", offs, cnt, len(buf))
}
