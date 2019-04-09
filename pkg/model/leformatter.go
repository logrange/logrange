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
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/logrange/pkg/utils/kvstring"
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
	frmtFldVar
	frmtFldVars
	frmtFldConst
)

// NewFormatParser returns FormatParser for the format string provided or returns an error if any.
//
// The following conventions is applied for the format string fstr: Special constructions like
// LogEvent's field names, tags and time-stamp format should be placed into curly braces '{', '}':
// {msg:<json>  	- LogEvent message (raw or json escaped)
// {ts} 			- LogEvent timestamp in RFC3339 format
// {vars}			- All known tags and fields associated with the LogEvent source
// {ts:<format>} 	- LogEvent timestamp in the format provided
// {vars:<tag or field name>}	- Value of the tag or filed name provided
//
// Example:
// 	"app:{vars:name}\t{ts:15:04:05.000}: {msg}" - will print app name associated with the tag "name",
//												then timestamp and the log message
// 	"{msg}" - will print the log message body only
//	"{vars} - {msg}"	- will print all tags and fields associated with the source and the message body
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
			// we are in open curly brace state. the sequence `{{` and `{}` considered as  escaped for '{' and '}' chars.
			if rune == '{' {
				if startIdx == i {
					// ok the '{' is part of constant, go ahead with them
					state = 0
					continue
				}
				return nil, fmt.Errorf("unexpected { without closing the previous one \"%s...\"", fstr[:i+1])
			}

			if rune == '}' {
				if startIdx == i {
					// ok the '}' is part of constant, go ahead with them
					state = 0
					continue
				}

				val := strings.Trim(fstr[startIdx:i], " ")
				cv := strings.ToLower(val)
				if cv == "msg" {
					fields = append(fields, formatField{frmtFldMsg, ""})
				} else if strings.HasPrefix(cv, "msg:") && (len(val[4:]) == 0 || val[4:] == "json") {
					fields = append(fields, formatField{frmtFldMsg, val[4:]})
				} else if cv == "vars" {
					fields = append(fields, formatField{frmtFldVars, ""})
				} else if cv == "ts" {
					fields = append(fields, formatField{frmtFldTs, time.RFC3339})
				} else if strings.HasPrefix(cv, "ts:") {
					fields = append(fields, formatField{frmtFldTs, val[3:]})
				} else if strings.HasPrefix(cv, "vars:") && len(val) > 5 {
					fields = append(fields, formatField{frmtFldVar, val[5:]})
				} else {
					return nil, fmt.Errorf("unknown field {%s}. Expected values are: {msg:<json>}, {vars}, {ts}, {ts:<time format>}, {vars:<tag or field name>}", val)
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
			s := lge.Msg.AsWeakString()
			if ff.value == "json" {
				s = utils.EscapeJsonStr(s)
			}
			buf.WriteString(s)
		case frmtFldVar:
			v := le.Fields.Value(ff.value)
			if v == "" {
				ts := fp.tagSet(tl)

				if ts != nil {
					v = ts.Tag(ff.value)
				}
			}
			buf.WriteString(v)
		case frmtFldVars:
			buf.WriteString(string(tl))
			if !le.Fields.IsEmpty() {
				buf.WriteString(kvstring.FieldsSeparator)
				buf.WriteString(le.Fields.AsKVString())
			}
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
		s1, err := tag.Parse(tl)
		if err != nil {
			s = nil
		} else {
			s = &s1
		}
		fp.tags[tl] = s
	}
	return s
}
