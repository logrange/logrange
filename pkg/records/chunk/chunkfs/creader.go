// Copyright 2018 The logrange Authors
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

package chunkfs

import (
	"encoding/binary"
	"io"
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
		if err == io.EOF {
			err = cr.dr.seekToEnd()
		}
		return err
	}

	offs := int64(binary.BigEndian.Uint64(offsBuf))
	return cr.dr.seek(offs)
}
