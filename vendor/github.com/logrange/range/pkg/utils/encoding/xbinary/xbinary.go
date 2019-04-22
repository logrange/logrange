// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xbinary

import (
	"encoding/binary"
	"fmt"
	"github.com/logrange/range/pkg/utils/bytes"
	"io"
)

type ObjectsWriter struct {
	Writer io.Writer
	buf    [10]byte
}

// WriteByte writes value v to the writer. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteByte(v byte) (int, error) {
	ow.buf[0] = v
	return ow.Writer.Write(ow.buf[:1])
}

// WriteUint16 writes value v to the writer. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteUint16(v uint16) (int, error) {
	buf := ow.buf[:2]
	binary.BigEndian.PutUint16(buf, v)
	return ow.Writer.Write(buf)
}

// WriteUint32 writes value v to the writer. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteUint32(v uint32) (int, error) {
	buf := ow.buf[:4]
	binary.BigEndian.PutUint32(buf, v)
	return ow.Writer.Write(buf)
}

// WriteUint64 writes value v to the writer. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteUint64(v uint64) (int, error) {
	buf := ow.buf[:8]
	binary.BigEndian.PutUint64(buf, v)
	return ow.Writer.Write(buf)
}

// WriteUint writes value v to the writer with the variable size. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteUint(v uint) (int, error) {
	sz, _ := MarshalUint(v, ow.buf[:])
	return ow.Writer.Write(ow.buf[:sz])
}

// WritePureBytes writes value v to the writer. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WritePureBytes(v []byte) (int, error) {
	return ow.Writer.Write(v)
}

// WritePureString writes value v to writer w. It returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WritePureString(v string) (int, error) {
	return ow.WritePureBytes(bytes.StringToByteArray(v))
}

// WriteBytes writes unmarshalable v to the writer. The written value could be unmarshaled by UnmarshalBytes,
// It contains a header - length of the bytes slice at the beginning. The method returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteBytes(v []byte) (int, error) {
	n, err := ow.WriteUint(uint(len(v)))
	if err != nil {
		return n, err
	}
	nn, err := ow.Writer.Write(v)
	return nn + n, err
}

// WriteString writes unmarshalable v to the writer. The written value could be unmarshaled by UnmarshalString,
// It contains a header - length of the bytes slice at the beginning. The method returns number of bytes written or an error if any.
func (ow *ObjectsWriter) WriteString(v string) (int, error) {
	return ow.WriteBytes(bytes.StringToByteArray(v))
}

// MarshalByte writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalByte(v byte, buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, noBufErr("MarshalByte", len(buf), 1)
	}
	buf[0] = v
	return 1, nil
}

// UnmarshalByte reads next byte value from the buf. Retruns number of bytes read, the value or an error, if any.
func UnmarshalByte(buf []byte) (int, byte, error) {
	if len(buf) == 0 {
		return 0, 0, noBufErr("UnmarshalByte", len(buf), 1)
	}
	return 1, buf[0], nil
}

// MarshalUint16 writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalUint16(v uint16, buf []byte) (int, error) {
	if len(buf) < 2 {
		return 0, noBufErr("MarshalUint16", len(buf), 2)
	}
	binary.BigEndian.PutUint16(buf, v)
	return 2, nil
}

// UnmarshalUint16 reads next uint16 value from the buf. Retruns number of bytes read, the value or an error, if any.
func UnmarshalUint16(buf []byte) (int, uint16, error) {
	if len(buf) < 2 {
		return 0, 0, noBufErr("UnmarshalUint16", len(buf), 2)
	}
	return 2, binary.BigEndian.Uint16(buf), nil
}

// MarshalUint32 writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalUint32(v uint32, buf []byte) (int, error) {
	if len(buf) < 4 {
		return 0, noBufErr("MarshalUint32", len(buf), 4)
	}
	binary.BigEndian.PutUint32(buf, v)
	return 4, nil
}

// UnmarshalUint32 reads next uint32 value from the buf. Retruns number of bytes read, the value or an error, if any.
func UnmarshalUint32(buf []byte) (int, uint32, error) {
	if len(buf) < 4 {
		return 0, 0, noBufErr("UnmarshalUint32", len(buf), 4)
	}
	return 4, binary.BigEndian.Uint32(buf), nil
}

// MarshalUint64 writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalUint64(v uint64, buf []byte) (int, error) {
	if len(buf) < 8 {
		return 0, noBufErr("MarshalUint64", len(buf), 8)
	}
	binary.BigEndian.PutUint64(buf, v)
	return 8, nil
}

// UnmarshalUint64 reads next int64 value from the buf. Retruns number of bytes read, the value or an error, if any.
func UnmarshalUint64(buf []byte) (int, uint64, error) {
	if len(buf) < 8 {
		return 0, 0, noBufErr("UnmarshalUint64", len(buf), 8)
	}
	return 8, binary.BigEndian.Uint64(buf), nil
}

// MarshalUint writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalUint(v uint, buf []byte) (int, error) {
	idx := 0
	for {
		if idx == len(buf) {
			return 0, noBufErr("MarshalUInt", len(buf), 8)
		}

		if v > 127 {
			buf[idx] = 128 | (byte(v & 127))
		} else {
			buf[idx] = byte(v)
			return idx + 1, nil
		}
		v = v >> 7
		idx++
	}
}

// UnmarshalUint reads next uint value from the buf. It retruns number of bytes read, the value or an error, if any.
func UnmarshalUint(buf []byte) (int, uint, error) {
	res := uint(0)
	idx := 0
	shft := uint(0)
	for {
		if idx == len(buf) {
			return 0, 0, noBufErr("UnmarshalUInt", len(buf), 8)
		}

		b := buf[idx]
		res = res | (uint(b&127) << shft)
		if b <= 127 {
			return idx + 1, res, nil
		}
		shft += 7
		idx++
	}
}

const (
	bit7  = 1 << 7
	bit14 = 1 << 14
	bit21 = 1 << 21
	bit28 = 1 << 28
	bit35 = 1 << 35
	bit42 = 1 << 42
	bit49 = 1 << 49
	bit56 = 1 << 56
	bit63 = 1 << 63
)

// WritableUintSize returns size of encoded uint64 size
func WritableUintSize(v uint64) int {
	if v >= bit35 {
		if v >= bit49 {
			if v >= bit63 {
				return 10
			}
			if v >= bit56 {
				return 9
			}
			return 8
		}
		if v >= bit42 {
			return 7
		}
		return 6
	}

	if v >= bit21 {
		if v >= bit28 {
			return 5
		}
		return 4
	}

	if v >= bit14 {
		return 3
	}

	if v >= bit7 {
		return 2
	}
	return 1
}

// MarshalBytes writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalBytes(v []byte, buf []byte) (int, error) {
	ln := len(v)

	idx, err := MarshalUint(uint(ln), buf)
	if err != nil {
		return 0, err
	}

	buf = buf[idx:]
	if len(buf) < ln {
		return 0, noBufErr("MarshalBytes-size-body", len(buf), ln)
	}

	copy(buf[:ln], v)
	return ln + idx, nil
}

// UnmarshalBytes reads next []byte value from the buf. It retruns number of bytes read, the value or an error, if any.
func UnmarshalBytes(buf []byte, newBuf bool) (int, []byte, error) {
	idx, uln, err := UnmarshalUint(buf)
	if err != nil {
		return 0, nil, err
	}

	ln := int(uln)
	if len(buf) < ln+idx {
		return 0, nil, noBufErr("UnmarshalBytes-size-body", len(buf)-idx, ln)
	}

	res := buf[idx : idx+ln]
	if newBuf {
		res = bytes.BytesCopy(res)
	}
	return idx + ln, res, nil
}

// WritebleBytesSize returns size of encoded buf
func WritebleBytesSize(buf []byte) int {
	return WritableUintSize(uint64(len(buf))) + len(buf)
}

// MarshalString writes value v to the buf. Returns number of bytes written or an error, if any
func MarshalString(v string, buf []byte) (int, error) {
	return MarshalBytes(bytes.StringToByteArray(v), buf)
}

// UnmarshalString reads next string value from the buf. It retruns number of bytes read, the value or an error, if any.
func UnmarshalString(buf []byte, newBuf bool) (int, string, error) {
	idx, res, err := UnmarshalBytes(buf, newBuf)
	if err == nil {
		return idx, bytes.ByteArrayToString(res), err
	}
	return idx, "", err
}

// WritableStringSize returns size of encoded v
func WritableStringSize(v string) int {
	return WritebleBytesSize(bytes.StringToByteArray(v))
}

func noBufErr(src string, ln, req int) error {
	return fmt.Errorf("not enough space in the buf: %s requres %d bytes, but actual buf size is %d", src, req, ln)
}
