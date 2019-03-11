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

package field

import (
	"strings"
)

type Field string

const delimChar = byte(0)

func newFieldFromMap(mp map[string]string) Field {
	var buf strings.Builder
	first := true
	for n, v := range mp {
		if !first {
			buf.WriteByte(delimChar)
		} else {
			first = false
		}
		buf.WriteString(n)
		buf.WriteByte(delimChar)
		buf.WriteString(v)
	}
	return Field(buf.String())
}

func (f Field) Value(name string) string {
	idx := 0
	var ok bool
	for idx >= 0 {
		ok, idx = f.check(idx, name)
		if idx == -1 {
			return ""
		}
		v, nidx := f.next(idx)
		if ok {
			return v
		}
		idx = nidx
	}
	return ""
}

func (f Field) check(i int, str string) (bool, int) {
	if len(f)-i <= len(str) {
		return false, -1
	}
	idx := i
	for ; idx < len(f); idx++ {
		if f[idx] == delimChar {
			return idx-i == len(str), idx + 1
		}
		sidx := idx - i
		if sidx >= len(str) || f[idx] != str[sidx] {
			_, idx = f.next(idx)
			return false, idx
		}
	}
	return idx-i == len(str), -1
}

func (f Field) next(i int) (string, int) {
	idx := i
	for ; idx < len(f); idx++ {
		if f[idx] == delimChar {
			return string(f[i:idx]), idx + 1
		}
	}
	return string(f[i:idx]), -1
}
