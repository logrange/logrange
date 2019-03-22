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

// kvstring package contains functions for representing string key:value pairs as one string and converting
// such strings to maps etc.
package kvstring

import (
	"fmt"
	"github.com/pkg/errors"
	"strconv"
)

const (
	KeyValueSeparator = "="
	FieldsSeparator   = ","
)

// RemoveCurlyBraces trims leading and trailing spaces and curly braces. For example input strings
// like ` { abc } ` will get as a result `abc` string
func RemoveCurlyBraces(str string) (string, error) {
	idx := 0
	cnt := 0
	for ; idx < len(str); idx++ {
		c := str[idx]
		if c == ' ' {
			continue
		}
		if c == '{' {
			cnt++
			continue
		}
		break
	}

	tidx := len(str) - 1
	for ; tidx > idx && cnt >= 0; tidx-- {
		c := str[tidx]
		if c == ' ' {
			continue
		}
		if c == '}' {
			cnt--
			continue
		}
		break
	}

	if tidx == idx || cnt != 0 {
		return str, fmt.Errorf("improperly formated tags string %s, expected format must be either {k=v, ...} or k=v,.. ", str)
	}

	return str[idx : tidx+1], nil
}

// SplitString receves key-value string where the key-value separator is kvSep, and the pairs are separated by fldSep.
// For example in the string `name=app1,ip=1234` the kvSep is '=' and fldSep is ','. The SplitString expects the result
// buf which will be used for preparing result. key or value could contain fldSep or kvSep chars, if the value is put into
// quotes.
func SplitString(str string, kvSep, fldSep byte, buf []string) ([]string, error) {
	inStr := false
	expCC := kvSep
	stIdx := 0
	endIdx := 0
	for ; endIdx < len(str); endIdx++ {
		c := str[endIdx]
		if c == '"' {
			inStr = !inStr
			continue
		}

		if c == '\\' && inStr {
			endIdx++
			continue
		}

		if (c == kvSep || c == fldSep) && !inStr {
			if c != expCC {
				return nil, fmt.Errorf("unexpected separator at %d of %s. Expected %c, but actually %c", endIdx, str, expCC, c)
			}
			if expCC == kvSep {
				expCC = fldSep
			} else {
				expCC = kvSep
			}
			buf = append(buf, str[stIdx:endIdx])
			stIdx = endIdx + 1

			continue
		}
	}
	if inStr {
		return nil, fmt.Errorf("unexpected end of string %s. Quotation %t is not closed", str, inStr)
	}

	buf = append(buf, str[stIdx:endIdx])

	return buf, nil
}

// ToMap turns a key-value string into map. For example the string `{ name=app, ccc="ddd" }` will be turned into
// a map, which equals to map[string]string{"name":"app", "ccc", "ddd"}
func ToMap(kvs string) (map[string]string, error) {
	fine, err := RemoveCurlyBraces(kvs)
	if err != nil {
		return nil, err
	}
	if len(fine) == 0 {
		return map[string]string{}, nil
	}

	var buf [40]string
	res, err := SplitString(fine, KeyValueSeparator[0], FieldsSeparator[0], buf[:0])
	if err != nil {
		return nil, err
	}

	if len(res)&1 == 1 {
		return nil, fmt.Errorf("the tag must be a pair of <key>=<value>")
	}

	mp := make(map[string]string, len(res)/2)
	for i := 0; i < len(res); i += 2 {
		k := TrimSpaces(res[i])
		v := TrimSpaces(res[i+1])
		if len(k) == 0 {
			return nil, errors.Errorf("tag name (for value=%s) could not be empty: %s", kvs, v)
		}

		if len(v) > 0 && (v[0] == '"' || v[0] == '`') {
			v1 := v
			v, err = strconv.Unquote(v)
			if err != nil {
				return nil, errors.Wrapf(err, "wrong value for tag \"%s\" which is %s, seems quotated, but could not unqote it", k, v1)
			}
		}
		mp[k] = v
	}

	return mp, nil
}

// MapsEquals returns whether m1 equals to m2
func MapsEquals(m1, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}

	return MapSubset(m1, m2)
}

// MapSubset returns whether m1 is subset of m2 (all m1's key-values are in m2 as well)
func MapSubset(m1, m2 map[string]string) bool {
	for k, v := range m1 {
		if v2, ok := m2[k]; !ok || v2 != v {
			return false
		}
	}
	return true
}

// trimSpaces removes leading and tailing spaces
func TrimSpaces(str string) string {
	i := 0
	for ; i < len(str); i++ {
		if str[i] == ' ' {
			continue
		}
		break
	}

	j := len(str) - 1
	for ; j > i; j-- {
		if str[j] == ' ' {
			continue
		}
		break
	}
	return str[i : j+1]
}
