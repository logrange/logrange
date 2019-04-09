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

package utils

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

var hex = "0123456789abcdef"

// ToJsonStr encodes v to a json string. Don't use it in data streaming due to
// bad performance and memory allocations
func ToJsonStr(v interface{}) string {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(v)
	if err != nil {
		return ""
	}
	s := string(buffer.Bytes())
	return strings.TrimRight(s, "\n")
}

// Provides the same escaping/encoding as standard json.Marshal,
// but does not escape HTML unsafe symbols, which are actually valid json
func EscapeJsonStr(s string) string {
	e := &bytes.Buffer{}
	e.WriteByte('"')

	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			// write bytes >= 0x20 as is (except for double quote " and back slash \)
			if b >= ' ' && b != '"' && b != '\\' {
				i++
				continue
			}

			if start < i {
				e.WriteString(s[start:i])
			}

			// escape \, ", \t, \n and \r
			e.WriteByte('\\')
			switch b {
			case '\\', '"':
				e.WriteByte(b)
			case '\n':
				e.WriteByte('n')
			case '\r':
				e.WriteByte('r')
			case '\t':
				e.WriteByte('t')
			default:
				// encode bytes < 0x20 (except for \t, \n and \r)
				e.WriteString(`u00`)
				e.WriteByte(hex[b>>4])
				e.WriteByte(hex[b&0xF])
			}

			i++
			start = i
			continue

		}

		c, size := utf8.DecodeRuneInString(s[i:])
		if c != utf8.RuneError {
			i += size
			continue
		}

		if size == 1 {
			if start < i {
				e.WriteString(s[start:i])
			}
			e.WriteString(`\ufffd`)
			i += size
			start = i
		}
	}

	if start < len(s) {
		e.WriteString(s[start:])
	}

	e.WriteByte('"')
	return string(e.Bytes())
}
