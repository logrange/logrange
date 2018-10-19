package logevent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type (
	// TagLine contains a list of tags in a form |tag1=val1|tag2=val2|...| the tags
	// are sorted alphabetically in ascending order
	TagLine string

	// TagMap is immutable storage where the key is the tag name and it is holded by its value
	TagMap map[string]string

	// An immutable structure which holds a reference to the TagMap
	Tags struct {
		gId int64
		tl  TagLine
		tm  TagMap
	}

	tagsJson struct {
		GID     int64   `json:"gid"`
		TagLine TagLine `json:"tagLine"`
	}
)

const (
	cTagValueSeparator = "="
	cTagSeparator      = "|"

	TAG_TIMESTAMP = "ts"
	TAG_MESSAGE   = "msg"
	TAG_JOURNAL   = "src"
)

var (
	EmptyTagMap = TagMap(map[string]string{})
)

func (tl *TagLine) NewTags(id int64) (Tags, error) {
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
			return m, fmt.Errorf("Wrong tag format: %s expecting in a form key=value", v)
		}
		m[kv[0]] = kv[1]
	}
	return m, nil
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

func (tm *TagMap) NewTags(id int64) (Tags, error) {
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

func (tags *Tags) GetId() int64 {
	return tags.gId
}

func (tags *Tags) GetTagLine() TagLine {
	return tags.tl
}

func (tags *Tags) GetValue(key string) string {
	return tags.tm[key]
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
