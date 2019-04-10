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

package model

import (
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
)

type (
	// LogEvent struct is used for storing one log-event record in a partition.
	LogEvent struct {
		// Timestamp of the message. It is filled by collector and can contain either collection timestamp or the
		// message timestamp
		Timestamp uint64

		// Msg - the message iteself. Usually it is a slice of bytes that refer to a temporary buffer
		Msg records.Record

		// Fields contains fields associated with the log event
		Fields field.Fields
	}
)

const recVersion = 0x20 // bits 5-7 contains version. bits 0-4 contains mask for the fields to be read

// Release drops references to the buffers if the logEvent has ones
func (le *LogEvent) Release() {
	le.Msg = nil
	le.Fields = ""
}

func (le *LogEvent) MakeItSafe() {
	le.Msg = le.Msg.MakeCopy()
	le.Fields = le.Fields.MakeCopy()
}

// WritableSize returns the number of bytes required to marshal the LogEvent
func (le *LogEvent) WritableSize() int {
	base := 1 + 8 + xbinary.WritebleBytesSize(le.Msg)
	if len(le.Fields) > 0 {
		base += xbinary.WritableStringSize(string(le.Fields))
	}
	return base
}

func (le *LogEvent) header() byte {
	if len(le.Fields) > 0 {
		return recVersion | 1
	}
	return recVersion
}

// WriteTo writes LogEvent le into ow
func (le *LogEvent) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	hdr := le.header()
	nn, err := ow.WriteByte(hdr)
	if err != nil {
		return nn, err
	}

	n, err := ow.WriteUint64(uint64(le.Timestamp))
	nn += n
	if err != nil {
		return nn, err
	}

	n, err = ow.WriteBytes(le.Msg)
	nn += n
	if hdr&1 != 0 && err == nil {
		n, err = ow.WriteString(string(le.Fields))
		nn += n
	}
	return nn, err
}

// Marshal encodes the log event into the buffer provided
func (le *LogEvent) Marshal(buf []byte) (int, error) {
	hdr := le.header()
	nn, err := xbinary.MarshalByte(hdr, buf)
	if err != nil {
		return 0, err
	}

	n, err := xbinary.MarshalUint64(uint64(le.Timestamp), buf[nn:])
	nn += n
	if err != nil {
		return nn, err
	}
	n, err = xbinary.MarshalBytes(le.Msg, buf[nn:])
	nn += n
	if hdr&1 != 0 && err == nil {
		n, err = xbinary.MarshalString(string(le.Fields), buf[nn:])
		nn += n
	}

	return nn, err
}

// Unmarshal reads the log event data from the buffer. It returns number of bytes read or an error if any
func (le *LogEvent) Unmarshal(buf []byte, newBuf bool) (int, error) {
	nn, hdr, err := xbinary.UnmarshalByte(buf)
	if err != nil {
		return nn, err
	}

	n, ts, err := xbinary.UnmarshalUint64(buf[nn:])
	nn += n
	if err != nil {
		return nn, err
	}

	le.Timestamp = ts
	n, le.Msg, err = xbinary.UnmarshalBytes(buf[nn:], newBuf)
	nn += n
	if hdr&1 != 0 && err == nil {
		var flds string
		n, flds, err = xbinary.UnmarshalString(buf[nn:], newBuf)
		nn += n
		if err == nil {
			le.Fields = field.Fields(flds)
		}
	}

	return nn, err
}
