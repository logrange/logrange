package records

import (
	"encoding/binary"
	"fmt"
)

type (
	// Writer allows to form Records structure in a slice of bytes.
	Writer struct {
		buf     []byte
		clsdPos int
		offs    int
		noExt   bool
	}
)

// Reset Initializes the writer by provided slice. The extendable flag
// allows the buffer will be re-allocated in case of not enough space in Allocate
// method (see below)
func (bbw *Writer) Reset(buf []byte, extendable bool) {
	bbw.buf = buf
	if cap(bbw.buf) > 0 {
		bbw.buf = bbw.buf[:cap(bbw.buf)]
	}
	bbw.offs = 0
	bbw.clsdPos = 0
	bbw.noExt = !extendable
}

// String - returns the string for the writer value
func (bbw *Writer) String() string {
	return fmt.Sprintf("{len(buf)=%d, clsdPos=%d, offs=%d, noExt=%t}", len(bbw.buf), bbw.clsdPos, bbw.offs, bbw.noExt)
}

// Allocate reserves ln bytes for data and writes its size before the data.
// The method returns slice of allocated bytes. In case of the allocation is not
// possible, it returns error.
//
// The extend param defines the caller desire to extend the buffer if the size is
// not big enough or return a error, without extension. If the buffer is not
// extendable, it will return an error in any case when the buffers size is insufficient.
// If the buffer is extendable, it will try to re-allocate the buffer only if the
// extend == true
func (bbw *Writer) Allocate(ln int, extend bool) ([]byte, error) {
	if bbw.clsdPos > 0 {
		return nil, fmt.Errorf("the writer already closed")
	}

	rest := len(bbw.buf) - bbw.offs - ln - 4
	if rest < 0 && (!extend || !bbw.extend(ln+4)) {
		return nil, fmt.Errorf("not enough space - available %d, but needed %d", len(bbw.buf)-bbw.offs, ln+4)
	}

	binary.BigEndian.PutUint32(bbw.buf[bbw.offs:], uint32(ln))
	bbw.offs += ln + 4
	return bbw.buf[bbw.offs-ln : bbw.offs], nil
}

// FreeLastAllocation releases the last allocation. The ln contains last allocation
// size. Will return error if the last allocation was not the pprovided size
func (bbw *Writer) FreeLastAllocation(ln int) error {
	pos := bbw.offs - ln - 4
	if pos < 0 || binary.BigEndian.Uint32(bbw.buf[pos:pos+4]) != uint32(ln) {
		return fmt.Errorf("Wrong last allocation size=%d bbw=%s", ln, bbw)
	}

	bbw.offs = pos
	return nil
}

// extend tries to extend the buffer if it is possbile to be able store at least
// ln bytes (including its size)
func (bbw *Writer) extend(ln int) bool {
	if bbw.noExt {
		return false
	}

	nsz := len(bbw.buf) * 3 / 2
	if bbw.offs+ln > nsz {
		nsz = len(bbw.buf) + ln*2
	}

	nb := make([]byte, nsz)
	copy(nb, bbw.buf)
	bbw.buf = nb
	return true
}

// Close puts EOF marker or completes the writing process. Consequentive
// Allocate() calls will return errors. The Close() method returns Records or
// an error if any
func (bbw *Writer) Close() (Records, error) {
	if bbw.clsdPos > 0 {
		return Records(bbw.buf[:bbw.clsdPos-1]), nil
	}

	if len(bbw.buf)-bbw.offs < 4 {
		bbw.clsdPos = bbw.offs + 1
		if bbw.buf != nil {
			bbw.buf = bbw.buf[:bbw.offs]
		}
		return Records(bbw.buf), nil
	}

	binary.BigEndian.PutUint32(bbw.buf[bbw.offs:], cEofMarker)
	bbw.clsdPos = bbw.offs + 1
	bbw.offs = len(bbw.buf)
	return Records(bbw.buf[:bbw.clsdPos-1]), nil
}
