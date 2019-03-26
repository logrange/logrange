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
	"fmt"
	"github.com/logrange/logrange/pkg/utils/kvstring"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

type Fields string

// NewFields makes new Fields value from the map provided
func NewFields(mp map[string]string) (Fields, error) {
	var buf strings.Builder
	for k, v := range mp {
		if len(k) > 255 || len(v) > 255 {
			return "", fmt.Errorf("field name or value cannot exceed 255 bytes")
		}
		buf.WriteByte(byte(len(k)))
		buf.WriteString(k)
		buf.WriteByte(byte(len(v)))
		buf.WriteString(v)
	}
	return Fields(buf.String()), nil
}

// Parse turns kvs to Fields and it doesn't care about error. If kvs is not right string the Fields will be
// silently empty
func Parse(kvs string) Fields {
	flds, _ := NewFieldsFromKVString(kvs)
	return flds
}

// NewFieldsFromKVString receives a kv-stream in form `field1=value1, field2=value2...` and turn
// it to Fields.
func NewFieldsFromKVString(kvs string) (Fields, error) {
	if len(kvs) == 0 {
		return "", nil
	}
	fine, err := kvstring.RemoveCurlyBraces(kvs)
	if err != nil {
		return "", err
	}
	if len(fine) == 0 {
		return "", nil
	}

	var strs [40]string
	res, err := kvstring.SplitString(fine, kvstring.KeyValueSeparator[0], kvstring.FieldsSeparator[0], strs[:0])
	if err != nil {
		return "", err
	}

	if len(res)&1 == 1 {
		return "", fmt.Errorf("the tag must be a pair of <key>=<value>")
	}

	var sb strings.Builder
	idx := 0
	for i, v := range res {
		if len(v) > 255 {
			return "", errors.Errorf("field name or value cannot exceed 255 bytes.")
		}
		v := kvstring.TrimSpaces(v)
		if len(v) == 0 && i&1 == 0 {
			return "", errors.Errorf("tag name (for value=%s) could not be empty: %s", kvs, v)
		}

		if len(v) > 0 && (v[0] == '"' || v[0] == '`') {
			v1 := v
			v, err = strconv.Unquote(v)
			if err != nil {
				return "", errors.Wrapf(err, "wrong value %s, seems quotated, but could not unqote it", v1)
			}
		}

		sb.WriteByte(byte(len(v)))
		idx++
		sb.WriteString(v)
		idx += len(v)
	}

	return Fields(sb.String()), nil
}

// Check tests whether the provided string is a properly coded fields or not
func Check(str string) (Fields, error) {
	idx := 0
	for idx < len(str) {
		n := int(str[idx])
		idx += n + 1
	}
	if idx != len(str) {
		return "", fmt.Errorf("inproperly formatted fields %s", str)
	}

	return Fields(str), nil
}

// Value returns the string value for a field name or empty string if not found
func (f Fields) Value(name string) string {
	idx := 0
	even := true
	for idx < len(f) {
		n := int(f[idx])
		if even && n == len(name) && string(f[idx+1:idx+n+1]) == name {
			idx += n + 1
			n := int(f[idx])
			return string(f[idx+1 : idx+n+1])
		}
		even = !even
		idx += n + 1
	}
	return ""
}

// MakeCopy makes an unmutable copy of fields
func (f Fields) MakeCopy() Fields {
	return Fields(bytes.ByteArrayToString(bytes.BytesCopy(bytes.StringToByteArray(string(f)))))
}

// Merge got fields f and merges them with values from map m. Result is written to Writer w and it returns
// result based on the buffer from w. The result cannot be stored in any collection, but copied only.
// Use the method with extra care.
func (f Fields) MergeWithMap(m map[string]string, w *bytes.Writer) Fields {
	w.Reset()
	if len(m) == 0 {
		return f
	}

	for i := 0; i < len(f); {
		k := int(f[i])
		v := int(f[i+k+1])
		key := string(f[i+1 : i+k+1])
		i += k + 2
		if _, ok := m[key]; !ok {
			w.WriteByte(byte(k))
			w.WriteString(key)
			w.WriteByte(byte(v))
			w.WriteString(string(f[i : i+v]))
		}
		i += v
	}

	for k, v := range m {
		w.WriteByte(byte(len(k)))
		w.WriteString(k)
		w.WriteByte(byte(len(v)))
		w.WriteString(v)
	}

	return Fields(bytes.ByteArrayToString(w.Buf()))
}

// Concat adds fields from f1 to f using writer's w buffer for the result. It just
// adds all fields from f1 to f, so if f contains some fields from f1, they will added as well
// Use the method with extra care.
func (f Fields) Concat(f1 Fields, w *bytes.Writer) Fields {
	w.Reset()
	w.WriteString(string(f))
	w.WriteString(string(f1))
	return Fields(bytes.ByteArrayToString(w.Buf()))
}

// IsEmpty returns whether the field list is empty
func (f Fields) IsEmpty() bool {
	return len(f) == 0
}

// AsKVString represents Fields as key-value string
func (f Fields) AsKVString() string {
	var sb strings.Builder
	idx := 0
	even := true
	for idx < len(f) {
		n := int(f[idx])
		if even {
			if idx > 0 {
				sb.WriteByte(kvstring.FieldsSeparator[0])
			}
			sb.WriteString(string(f[idx+1 : idx+1+n]))
			sb.WriteByte(kvstring.KeyValueSeparator[0])
		} else {
			v := string(f[idx+1 : idx+1+n])
			if strings.IndexByte(v, kvstring.FieldsSeparator[0]) >= 0 || strings.IndexByte(v, kvstring.KeyValueSeparator[0]) >= 0 {
				v = strconv.Quote(v)
			}
			sb.WriteString(v)
		}
		even = !even
		idx += n + 1
	}
	return sb.String()
}
