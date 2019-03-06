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

package tag

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"sort"
	"strconv"
	"strings"
)

type (
	Line string

	// tagMap is immutable storage where the key is the tag name and it is holded by its value
	tagMap map[string]string

	Set struct {
		line Line
		tmap tagMap
	}
)

const (
	cTagValueSeparator = "="
	cTagSeparator      = ","
)

var (
	emptyMap = tagMap(map[string]string{})
	emptySet = Set{"", emptyMap}
)

// Parse expects a string in format either "{name=value,name2=value...}" or
// "name=value,name2=value..." and returns the Set object or an error, if any.
//
// The value for any tag could be in escaped (double quoted by "). This case the value
// can contain the following symbols '{', '}', ',', '\', '"' escaped by backslash
func Parse(tags string) (Set, error) {
	if len(tags) == 0 {
		return emptySet, nil
	}

	tm, err := parseTags(tags)
	if err != nil {
		return emptySet, err
	}

	return Set{tm.line(), tm}, nil
}

// MapToSet receives a map of values mp and returns the Set of tags, formed from there.
func MapToSet(mp map[string]string) Set {
	if len(mp) == 0 {
		return emptySet
	}

	tm := make(tagMap, len(mp))
	for k, v := range mp {
		tm[k] = v
	}

	return Set{tm.line(), tm}
}

// Line returns formatted tags in sorted order
func (s *Set) Line() Line {
	return s.line
}

// Tag returns the tag value
func (s *Set) Tag(tag string) string {
	return s.tmap[tag]
}

// IsEmpty returns true if the set is empty
func (s *Set) IsEmpty() bool {
	return len(s.tmap) == 0
}

// SubsetOf returns whether all tags from s present in s1
func (s *Set) SubsetOf(s1 Set) bool {
	return s.tmap.subsetOf(s1.tmap)
}

// Equals returns whether set s is equal to s1
func (s *Set) Equals(s1 Set) bool {
	return s.line == s1.line
}

// String returns line of tags
func (s *Set) String() string {
	return string(s.line)
}

// MarshalJSON to support json.Marshaller interface
func (s *Set) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s.line))
}

// UnmarshalJSON to support json.Unmarshaller interface
func (s *Set) UnmarshalJSON(buf []byte) error {
	var ln string
	err := json.Unmarshal(buf, &ln)
	if err == nil && len(ln) > 0 {
		*s, err = Parse(ln)
	} else {
		*s = emptySet
	}
	return err
}

func (l Line) String() string {
	return string(l)
}

func (m tagMap) equalTo(m2 tagMap) bool {
	if len(m) != len(m2) {
		return false
	}

	return m.subsetOf(m2)
}

func (m tagMap) subsetOf(m2 tagMap) bool {
	for k, v := range m {
		if v2, ok := m2[k]; !ok || v2 != v {
			return false
		}
	}
	return true
}

func (m tagMap) line() Line {
	srtKeys := make([]string, 0, len(m))
	// sort keys
	for k := range m {
		idx := sort.SearchStrings(srtKeys, k)
		srtKeys = append(srtKeys, k)
		if idx < len(srtKeys)-1 {
			copy(srtKeys[idx+1:], srtKeys[idx:])
		}
		srtKeys[idx] = k
	}

	var b bytes.Buffer
	first := true
	for _, k := range srtKeys {
		if !first {
			b.WriteString(cTagSeparator)
		}
		b.WriteString(k)
		b.WriteString(cTagValueSeparator)
		v := m[k]
		if len(v) == 0 || strings.IndexByte(v, cTagValueSeparator[0]) >= 0 || strings.IndexByte(v, cTagSeparator[0]) >= 0 {
			v = strconv.Quote(v)
		}
		b.WriteString(v)
		first = false
	}
	return Line(b.String())
}

func parseTags(tags string) (tagMap, error) {
	fine, err := removeCurlyBraces(tags)
	if err != nil {
		return nil, err
	}
	if len(fine) == 0 {
		return emptyMap, nil
	}

	var buf [40]string
	res, err := splitString(fine, cTagValueSeparator[0], cTagSeparator[0], buf[:0])
	if err != nil {
		return nil, err
	}

	if len(res)&1 == 1 {
		return nil, fmt.Errorf("the tag must be a pair of <key>=<value>")
	}

	mp := make(tagMap, len(res)/2)
	for i := 0; i < len(res); i += 2 {
		k := trimSpaces(res[i])
		v := trimSpaces(res[i+1])
		if len(k) == 0 {
			return nil, errors.Errorf("tag name (for value=%s) could not be empty: %s", tags, v)
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

func splitString(str string, cc1, cc2 byte, buf []string) ([]string, error) {
	inStr := false
	expCC := cc1
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

		if (c == cc1 || c == cc2) && !inStr {
			if c != expCC {
				return nil, fmt.Errorf("unexpected separator at %d of %s. Expected %c, but actually %c", endIdx, str, expCC, c)
			}
			if expCC == cc1 {
				expCC = cc2
			} else {
				expCC = cc1
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

func trimSpaces(str string) string {
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

// removeCurlyBraces trims leadin and trailing spaces and curle braces
func removeCurlyBraces(str string) (string, error) {
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
