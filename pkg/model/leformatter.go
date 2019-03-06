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

package model

import (
	"fmt"
	"github.com/logrange/logrange/pkg/model/tag"
	"strings"
	"time"
)

type (
	formatField struct {
		typ   int
		value string
	}

	// FormatParser struct provides simple LogEvent's formatting functionality. The FormatParser
	// could be used for formatting log-events in lql.
	FormatParser struct {
		fields []formatField
		tags   map[string]*tag.Set
	}
)

const (
	frmtFldTs = iota
	frmtFldMsg
	frmtFldTag
	frmtFldTags
	frmtFldConst
)

// NewFormatParser returns FormatParser for the format string provided or returns an error if any.
//
// The following conventions is applied for the format string fstr: Special constructions like
// LogEvent's field names, tags and time-stamp format should be placed into curly braces '{', '}':
// {msg} 	- LogEvent message
// {ts} 	- LogEvent timestamp in RFC3339 format
// {tags}	- All known tags associated with the LogEvent source
// {ts:<format>} 	- LogEvent timestamp in the format provided
// {tag:<tag-name>}	- Value of the tag
//
// Example:
// 	"app:{tag:name}\t{ts:15:04:05.000}: {msg}" - will print app name associated with the tag "name",
//												then timestamp and the log message
// 	"{msg}" - will print the log message body only
//	"{tags} - {msg}"	- will print all tags associated with the source and the message body
func NewFormatParser(fstr string) (*FormatParser, error) {
	fields := make([]formatField, 0, 10)
	state := 0
	startIdx := 0
	for i, rune := range fstr {
		switch state {
		case 0:
			if rune == '{' {
				if i-startIdx > 0 {
					fields = append(fields, formatField{frmtFldConst, fstr[startIdx:i]})
				}
				state = 1
				startIdx = i + 1
			}
		case 1:
			if rune == '}' {
				val := strings.Trim(fstr[startIdx:i], " ")
				cv := strings.ToLower(val)
				if cv == "msg" {
					fields = append(fields, formatField{frmtFldMsg, ""})
				} else if cv == "tags" {
					fields = append(fields, formatField{frmtFldTags, ""})
				} else if cv == "ts" {
					fields = append(fields, formatField{frmtFldTs, time.RFC3339})
				} else if strings.HasPrefix(cv, "ts:") {
					fields = append(fields, formatField{frmtFldTs, val[3:]})
				} else if strings.HasPrefix(cv, "tag:") {
					fields = append(fields, formatField{frmtFldTag, val[4:]})
				} else {
					return nil, fmt.Errorf("unknown field {%s}. Expected values are: {msg}, {tags}, {ts}, {ts:<time format>}, {tag:<tag name>}", val)
				}
				startIdx = i + 1
				state = 0
			}
		}
	}

	if state != 0 {
		return nil, fmt.Errorf("unexpected end of string, '}' is not found.")
	}

	if startIdx < len(fstr) {
		fields = append(fields, formatField{frmtFldConst, fstr[startIdx:]})
	}

	return &FormatParser{fields: fields}, nil
}

// FormatStr formats provided log event and the tag line
func (fp *FormatParser) FormatStr(le *LogEvent, tl string) string {
	var lge LogEvent
	if le != nil {
		lge = *le
	}
	var buf strings.Builder
	for _, ff := range fp.fields {
		switch ff.typ {
		case frmtFldTs:
			if len(ff.value) > 0 {
				buf.WriteString(time.Unix(0, int64(lge.Timestamp)).Format(ff.value))
			}
		case frmtFldMsg:
			buf.WriteString(lge.Msg)
		case frmtFldTag:
			ts := fp.tagSet(tl)
			if ts != nil {
				buf.WriteString(ts.Tag(ff.value))
			}
		case frmtFldTags:
			buf.WriteString(string(tl))
		case frmtFldConst:
			buf.WriteString(ff.value)
		}
	}
	return buf.String()
}

func (fp *FormatParser) tagSet(tl string) *tag.Set {
	if fp.tags == nil {
		fp.tags = make(map[string]*tag.Set)
	}
	s, ok := fp.tags[tl]
	if !ok {
		s1, err := tag.Parse(string(tl))
		if err != nil {
			s = nil
		} else {
			s = &s1
		}
		fp.tags[tl] = s
	}
	return s
}
