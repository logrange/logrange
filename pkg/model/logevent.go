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

package model

import (
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
)

type (
	// LogEvent struct is used for storing one log-event record in a journal.
	LogEvent struct {
		// Timestamp of the message. It is filled by collector and can contain either collection timestamp or the
		// message timestamp
		Timestamp uint64

		// Msg - the message iteself
		Msg string
	}
)

// WritableSize returns the number of bytes required to marshal the LogEvent
func (le *LogEvent) WritableSize() int {
	return 8 + xbinary.WritableStringSize(le.Msg)
}

func (le *LogEvent) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	n, err := ow.WriteUint64(uint64(le.Timestamp))
	nn := n
	if err != nil {
		return nn, err
	}

	n, err = ow.WriteString(le.Msg)
	nn += n
	if err != nil {
		return nn, err
	}
	return nn, err
}

// Marshal encodes the log event into the buffer provided
func (le *LogEvent) Marshal(buf []byte) (int, error) {
	n, err := xbinary.MarshalUint64(uint64(le.Timestamp), buf)
	if err != nil {
		return 0, err
	}
	nn := n
	n, err = xbinary.MarshalString(le.Msg, buf[n:])
	return nn + n, err
}

// Unmarshal reads the log event data from the buffer. It returns number of bytes read or an error if any
func (le *LogEvent) Unmarshal(buf []byte, newBuf bool) (int, error) {
	n, ts, err := xbinary.UnmarshalUint64(buf)
	if err != nil {
		return n, err
	}
	le.Timestamp = ts
	nn := n
	n, le.Msg, err = xbinary.UnmarshalString(buf[n:], newBuf)
	return nn + n, err
}
