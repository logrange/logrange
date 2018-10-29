package logevent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/logrange/logrange/pkg/records/inmem"
)

type (
	SSlice []WeakString

	// WeakString is a string which probably points to a byte slice,
	// which context can be changed. The WeakString can be easily created without
	// memory allocation, but has to be used with an extra care, must not be
	// stored in a long-live collections or passed through a channel etc.
	WeakString string
)

// String turns the WeakString to it's safe immutable version. Just copy context
func (ws WeakString) String() string {
	return string(inmem.StringToByteArray(string(ws)))
}

func StrSliceToSSlice(ss []string) SSlice {
	return *(*SSlice)(unsafe.Pointer(&ss))
}

// Size() returns how much memory serialization needs
func (ss SSlice) Size() int {
	sz := 2
	for _, s := range ss {
		sz += 4 + len(s)
	}
	return sz
}

func MarshalSSlice(ss SSlice, buf []byte) (int, error) {
	idx, err := MarshalUint16(uint16(len(ss)), buf)
	if err != nil {
		return 0, err
	}

	for _, s := range ss {
		n, err := MarshalString(string(s), buf[idx:])
		if err != nil {
			return 0, err
		}
		idx += n
	}
	return idx, nil
}

func UnmarshalSSlice(ss SSlice, buf []byte) (SSlice, int, error) {
	idx, usz, err := UnmarshalUint16(buf)
	if err != nil {
		return nil, 0, err
	}

	sz := int(usz)
	if sz > cap(ss) {
		return nil, 0, errors.New(fmt.Sprintf("Not enough space in the result slice, required capacity is %d, but actual one is %d", sz, cap(ss)))
	}

	ss = ss[:sz]
	for i := 0; i < sz; i++ {
		n, s, err := UnmarshalString(buf[idx:])
		if err != nil {
			return nil, 0, err
		}
		idx += n
		ss[i] = s
	}
	return ss, idx, nil
}

func MarshalByte(v byte, buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, noBufErr("MarshalByte", len(buf), 1)
	}
	buf[0] = v
	return 1, nil
}

func UnmarshalByte(buf []byte) (int, byte, error) {
	if len(buf) == 0 {
		return 0, 0, noBufErr("UnmarshalByte", len(buf), 1)
	}
	return 1, buf[0], nil
}

func MarshalUint16(v uint16, buf []byte) (int, error) {
	if len(buf) < 2 {
		return 0, noBufErr("MarshalUint16", len(buf), 2)
	}
	binary.BigEndian.PutUint16(buf, v)
	return 2, nil
}

func UnmarshalUint16(buf []byte) (int, uint16, error) {
	if len(buf) < 2 {
		return 0, 0, noBufErr("UnmarshalUint16", len(buf), 2)
	}
	return 2, binary.BigEndian.Uint16(buf), nil
}

func MarshalUint32(v uint32, buf []byte) (int, error) {
	if len(buf) < 4 {
		return 0, noBufErr("MarshalUint32", len(buf), 4)
	}
	binary.BigEndian.PutUint32(buf, v)
	return 4, nil
}

func UnmarshalUint32(buf []byte) (int, uint32, error) {
	if len(buf) < 4 {
		return 0, 0, noBufErr("UnmarshalUint32", len(buf), 4)
	}
	return 4, binary.BigEndian.Uint32(buf), nil
}

func MarshalInt64(v int64, buf []byte) (int, error) {
	if len(buf) < 8 {
		return 0, noBufErr("MarshalInt64", len(buf), 8)
	}
	binary.BigEndian.PutUint64(buf, uint64(v))
	return 8, nil
}

func UnmarshalInt64(buf []byte) (int, int64, error) {
	if len(buf) < 8 {
		return 0, 0, noBufErr("UnmarshalInt64", len(buf), 8)
	}
	return 8, int64(binary.BigEndian.Uint64(buf)), nil
}

func MarshalString(v string, buf []byte) (int, error) {
	bl := len(buf)
	ln := len(v)
	if ln+4 > bl {
		return 0, noBufErr("MarshalString-size-body", bl, ln+4)
	}
	binary.BigEndian.PutUint32(buf, uint32(ln))
	var src = buf[4 : ln+4]
	dst := src
	src = *(*[]byte)(unsafe.Pointer(&v))
	copy(dst, src)
	return ln + 4, nil
}

// UnmarshalString fastest, but not completely safe version of unmarshalling
// the byte buffer to string. Please use with care and keep in mind that buf must not
// be updated so as it will affect the string context then.
func UnmarshalString(buf []byte) (int, WeakString, error) {
	if len(buf) < 4 {
		return 0, "", noBufErr("UnmarshalString-size", len(buf), 4)
	}
	ln := int(binary.BigEndian.Uint32(buf))
	if ln+4 > len(buf) {
		return 0, "", noBufErr("UnmarshalString-body", len(buf), ln+4)
	}
	bs := buf[4 : ln+4]
	res := *(*string)(unsafe.Pointer(&bs))
	return ln + 4, WeakString(res), nil
}

func MarshalStringBuf(v string, buf []byte) (int, error) {
	if len(v) > len(buf) {
		return 0, fmt.Errorf("could not MarshalStringBuf() - not enough space. Required %d bytes, but the buffer sized is %d", len(v), len(buf))
	}
	ba := inmem.StringToByteArray(v)
	return copy(buf, ba), nil
}

func UnmarshalStringBuf(buf []byte) WeakString {
	return WeakString(inmem.ByteArrayToString(buf))
}

func noBufErr(src string, ln, req int) error {
	return fmt.Errorf("not enough space in the buf: %s requres %d bytes, but actual buf size is %d", src, req, ln)
}
