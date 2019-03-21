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
	"encoding/json"
	"github.com/logrange/logrange/pkg/utils/kvstring"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/utils/bytes"
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

var (
	EmptyLine = Line("")
	emptyMap  = tagMap(map[string]string{})
	emptySet  = Set{"", emptyMap}
)

// ParseUnsafe expects a string in format either "{name=value,name2=value...}" or
// "name=value,name2=value..." and returns the Set object or an error, if any.
// The ParseUnsafe forms the result from the string tags, and it tries to avoid new
// allocations. If the passed string is not safe
//
// The value for any tag could be in escaped (double quoted by "). This case the value
// can contain the following symbols '{', '}', ',', '\', '"' escaped by backslash
func ParseUnsafe(tags records.Record) (Set, error) {
	if len(tags) == 0 {
		return emptySet, nil
	}

	m, err := kvstring.ToMap(bytes.ByteArrayToString(tags))
	if err != nil {
		return emptySet, err
	}
	tm := tagMap(m)

	return Set{tm.line(), tm}, nil
}

// Parse parses tags and make a copy of tags
func Parse(tags string) (Set, error) {
	return ParseUnsafe([]byte(tags))
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
		*s, err = ParseUnsafe(bytes.StringToByteArray(ln))
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
	return kvstring.MapSubset(m, m2)
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

	var sb strings.Builder
	first := true
	for _, k := range srtKeys {
		if !first {
			sb.WriteString(kvstring.FieldsSeparator)
		}
		sb.WriteString(k)
		sb.WriteString(kvstring.KeyValueSeparator)
		v := m[k]
		if len(v) == 0 || strings.IndexByte(v, kvstring.KeyValueSeparator[0]) >= 0 || strings.IndexByte(v, kvstring.FieldsSeparator[0]) >= 0 {
			v = strconv.Quote(v)
		}
		sb.WriteString(v)
		first = false
	}
	return Line(sb.String())
}
