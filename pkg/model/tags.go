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
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type (
	// TagLine contains a list of tags in a form tag1=val1|tag2=val2|... the tags
	// are sorted alphabetically in ascending order
	TagLine string

	// TagMap is immutable storage where the key is the tag name and it is holded by its value
	TagMap map[string]string

	// An immutable structure which holds a reference to the TagMap
	Tags struct {
		gId uint64
		tl  TagLine
		tm  TagMap
	}
)

const (
	cTagValueSeparator = "="
	cTagSeparator      = "|"
)

var (
	EmptyTagMap = TagMap(map[string]string{})
)

func (tl *TagLine) NewTags(id uint64) (Tags, error) {
	if *tl == "" {
		return Tags{gId: id, tl: *tl, tm: EmptyTagMap}, nil
	}
	m, err := tl.newTagMap()
	if err != nil {
		return Tags{}, err
	}

	return Tags{gId: id, tl: m.BuildTagLine(), tm: m}, nil
}

func (tl *TagLine) newTagMap() (TagMap, error) {
	vals := strings.Split(string(*tl), cTagSeparator)
	m := make(TagMap, len(vals))
	for _, v := range vals {
		kv := strings.Split(v, cTagValueSeparator)
		if len(kv) != 2 {
			return m, fmt.Errorf("Wrong tag format: \"%s\" expecting in a form key=value", v)
		}
		m[kv[0]] = kv[1]
	}
	return m, nil
}

func CheckTags(tgs string) (TagLine, error) {
	if len(tgs) == 0 {
		return "", nil
	}
	vals := strings.Split(tgs, cTagSeparator)
	for _, v := range vals {
		if len(strings.Split(v, cTagValueSeparator)) != 2 {
			return "", fmt.Errorf("Wrong tag format: \"%s\" expecting in a form key=value", v)
		}
	}
	return TagLine(tgs), nil
}

func NewTagMap(m map[string]string) (TagMap, error) {
	tm := make(TagMap, len(m))
	for k, v := range m {
		key := strings.ToLower(k)
		if _, ok := tm[key]; ok {
			return nil, fmt.Errorf("Incorrect tag initializing map, expecting keys to be case insensitive, but it is %v", m)
		}
		tm[key] = v
	}
	return tm, nil
}

func (tm *TagMap) NewTags(id uint64) (Tags, error) {
	return Tags{gId: id, tl: tm.BuildTagLine(), tm: *tm}, nil
}

// BuildTagLine builds the TagLine from the map of values
func (tm *TagMap) BuildTagLine() TagLine {
	srtKeys := make([]string, 0, len(*tm))
	// sort keys
	for k := range *tm {
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
		b.WriteString((*tm)[k])
		first = false
	}
	return TagLine(b.String())
}

func (tags *Tags) GetId() uint64 {
	return tags.gId
}

func (tags *Tags) GetTagLine() TagLine {
	return tags.tl
}

func (tags *Tags) GetValue(key string) string {
	return tags.tm[key]
}

type tagsJson struct {
	GID     uint64  `json:"gid"`
	TagLine TagLine `json:"tagLine"`
}

func (tags *Tags) MarshalJSON() ([]byte, error) {
	return json.Marshal(&tagsJson{tags.gId, tags.tl})
}

func (tags *Tags) UnmarshalJSON(data []byte) error {
	var res tagsJson
	err := json.Unmarshal(data, &res)
	if err != nil {
		return err
	}
	tags.gId = res.GID
	tags.tl = TagLine(res.TagLine)
	tags.tm, err = tags.tl.newTagMap()
	return err
}

func (tags *Tags) String() string {
	return fmt.Sprintf("{tid=%d, tl=%s}", tags.gId, tags.tl)
}
