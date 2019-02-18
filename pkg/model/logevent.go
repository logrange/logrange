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
		Timestamp int64
		TgId      int64
		Msg       string
		Tags      string
	}
)

// WritableSize returns the number of bytes required to marshal the LogEvent
func (le *LogEvent) WritableSize() int {
	// the fields mask goes first, it is 1 byte
	res := 1
	if le.Timestamp != 0 {
		res += 8
	}
	if le.TgId != 0 {
		res += 8
	}
	if len(le.Msg) > 0 {
		res += xbinary.WritableStringSize(le.Msg)
	}
	if len(le.Tags) > 0 {
		res += xbinary.WritableStringSize(le.Tags)
	}
	return res
}

func (le *LogEvent) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	var fmsk byte
	if le.Timestamp != 0 {
		fmsk = 1
	}
	if le.TgId != 0 {
		fmsk |= 2
	}
	if len(le.Msg) > 0 {
		fmsk |= 4
	}
	if len(le.Tags) > 0 {
		fmsk |= 8
	}

	n, err := ow.WriteByte(fmsk)
	if err != nil {
		return n, err
	}
	nn := n

	if fmsk&1 != 0 {
		n, err = ow.WriteUint64(uint64(le.Timestamp))
		nn += n
		if err != nil {
			return nn, err
		}
	}

	if fmsk&2 != 0 {
		n, err = ow.WriteUint64(uint64(le.TgId))
		nn += n
		if err != nil {
			return nn, err
		}
	}

	if fmsk&4 != 0 {
		n, err = ow.WriteString(le.Msg)
		nn += n
		if err != nil {
			return nn, err
		}
	}

	if fmsk&8 != 0 {
		n, err = ow.WriteString(le.Tags)
		nn += n
	}
	return nn, err
}

// Marshal encodes the log event into the buffer provided
func (le *LogEvent) Marshal(buf []byte) (int, error) {
	// the field mask goes first
	var fmsk byte
	idx := 1

	if le.Timestamp != 0 {
		fmsk |= 1
		n, err := xbinary.MarshalInt64(le.Timestamp, buf[idx:])
		if err != nil {
			return 0, err
		}
		idx += n
	}

	if le.TgId != 0 {
		fmsk |= 2
		n, err := xbinary.MarshalInt64(le.TgId, buf[idx:])
		if err != nil {
			return 0, err
		}
		idx += n
	}

	if len(le.Msg) > 0 {
		fmsk |= 4
		n, err := xbinary.MarshalString(le.Msg, buf[idx:])
		if err != nil {
			return 0, err
		}
		idx += n
	}

	if len(le.Tags) > 0 {
		fmsk |= 8
		n, err := xbinary.MarshalString(le.Tags, buf[idx:])
		if err != nil {
			return 0, err
		}
		idx += n
	}

	buf[0] = fmsk
	return idx, nil
}

// Unmarshal reads the log event data from the buffer. It returns number of bytes read or an error if any
func (le *LogEvent) Unmarshal(buf []byte, newBuf bool) (int, error) {
	idx, fmsk, err := xbinary.UnmarshalByte(buf)
	if err != nil {
		return idx, err
	}

	le.TgId = 0
	le.Timestamp = 0
	le.Tags = ""
	le.Msg = ""

	var n int
	if fmsk&1 != 0 {
		n, le.Timestamp, err = xbinary.UnmarshalInt64(buf[idx:])
		if err != nil {
			return idx, err
		}
		idx += n
	}

	if fmsk&2 != 0 {
		n, le.TgId, err = xbinary.UnmarshalInt64(buf[idx:])
		if err != nil {
			return idx, err
		}
		idx += n
	}

	if fmsk&4 != 0 {
		n, le.Msg, err = xbinary.UnmarshalString(buf[idx:], newBuf)
		if err != nil {
			return idx, err
		}
		idx += n
	}

	if fmsk&8 != 0 {
		n, le.Tags, err = xbinary.UnmarshalString(buf[idx:], newBuf)
		if err != nil {
			return idx, err
		}
		idx += n
	}

	return idx, nil
}
