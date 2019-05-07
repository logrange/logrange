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

package date

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

type (
	// Parser parses the given byte slice in accordance with the date formats
	// it has, returns parsed out date (leftmost only) and format which can be used to parse
	// similarly formatted dates. In case if no suitable format can be found
	// empty time.Time object is returned.
	//
	// NOTE: Since date portion search in the given slice is based on regexp
	// it can consume considerable amount of CPU cycles (depends on the number of
	// formats the current parser has).
	Parser interface {
		Parse(buf []byte) (time.Time, *Format)
	}

	Format struct {
		frmt        string         // user friendly format (e.g. YYYY/MM/DD)
		dLayout     string         // go-lang layout of frmt
		dRegexp     *regexp.Regexp // regexp to determine given format in a line
		hasLocation bool           // if format doesn't have location UTC is used
		hasYear     bool           // if format doesn't have year, current/previous year is used
		noDate      bool           // if format doesn't have date, usually time only like 'hh:mm:ss.SSS'
	}

	parser struct {
		Parser
		formats []*Format
	}

	// term structure describes transformation from user friendly
	// term format (e.g. YYYY/MM/DD) to go-lang term layout (2006/01/02)
	// and a regular expression that can be used for finding the term
	// (i.e. with this format/layout) in a text.
	term struct {
		format string
		layout string
		expr   string
	}
)

const (
	dateGroup = "date"
)

var (
	KnownFormats = []string{
		"MMM D, YYYY h:mm:ss P",
		"DDD MMM _D HH:mm:ss YYYY",
		"DDD MMM _D HH:mm:ss MST YYYY",
		"DDD MMM DD HH:mm:ss ZZZZ YYYY",
		"DDDD, YY-MMM-DD HH:mm:ss ZZZ",
		"DDD, DD MMM YYYY HH:mm:ss ZZZ",
		"DDD, DD MMM YYYY HH:mm:ss ZZZZ",
		"DDD, D MMM YYYY HH:mm:ss ZZZZ",
		"DD MMM YYYY, HH:mm",
		"YYYY-MMM-DD",
		"DD MMMM YYYY",

		// mm/dd/yy
		"DD/MM/YYYY HH:mm:ss.SSS",
		"DD/MM/YYYY HH:mm:ss",
		"D/MM/YYYY HH:mm:ss",
		"DD/M/YYYY HH:mm:ss",
		"D/M/YYYY HH:mm:ss",
		"D/M/YYYY hh:mm:ss P",
		"DD/MM/YYYY HH:mm",
		"D/M/YYYY HH:mm",
		"D/M/YY HH:mm",
		"D/M/YYYY hh:mm P",
		"D/M/YYYY h:mm P",
		"DD/MMM/YYYY:HH:mm:ss ZZZZ",
		"DD/MM/YYYY",
		"D/MM/YYYY",
		"DD/MM/YY",
		"D/M/YY",

		// yyyy/mm/dd
		"YYYY/MM/DD HH:mm:ss.SSS",
		"YYYY/MM/DD HH:mm:ss",
		"YYYY/MM/D HH:mm:ss",
		"YYYY/M/DD HH:mm:ss",
		"YYYY/MM/DD HH:mm",
		"YYYY/M/D HH:mm",
		"YYYY/MM/DD",
		"YYYY/M/DD",

		// yyyy-mm-ddThh
		"YYYY-MM-DDTHH:mm:ss.SSSZZZZ",
		"YYYY-MM-DDTHH:mm:ss.SSSZ",
		"YYYY-MM-DDTHH:mm:ssZZZZZ",
		"YYYY-MM-DDTHH:mm:ssZZZZ",
		"YYYY-MM-DDTHH:mm:ssZ",
		"YYYY-MM-DDTHH:mm:ss",

		// yyyy-mm-dd hh:mm:ss
		"YYYY-MM-DD HH:mm:ss.SSS ZZZZ ZZZ",
		"YYYY-MM-DD HH:mm:ss.SSS",
		"YYYY-MM-DD HH:mm:ss ZZZZZ",
		"YYYY-MM-DD HH:mm:ssZZZZZ",
		"YYYY-MM-DD HH:mm:ss ZZZZ ZZZ",
		"YYYY-MM-DD HH:mm:ss ZZZZ",
		"YYYY-MM-DD HH:mm:ss ZZZ",
		"YYYY-MM-DD hh:mm:ss P",
		"YYYY-MM-DD HH:mm:ss",
		"YYYY-MM-DD  HH:mm:ss",
		"YYYY-MM-DD HH:mm",
		"YYYY-MM-DD",

		// mm.dd.yy
		"MM.DD.YYYY",
		"MM.DD.YY",

		// no year
		"DDD MMM _D HH:mm:ss.SSS",
		"DDD MMM DD HH:mm:ss.SSS",
		"MMM DD HH:mm:ss",
		"MMM _D HH:mm:ss",
	}

	// Descending order of the 'alike' symbols is important
	// we're going to do the replacements in the given order,
	// so we want the 'larger' terms to be replaced first...
	terms = []term{
		{"YYYY", "2006", "[1-2]\\d{3}"},
		{"YY", "06", "\\d{2}"},
		{"MMMM", "January", "[A-Z][a-z]{2,8}"},
		{"MMM", "Jan", "[A-Z][a-z]{2}"},
		{"MM", "01", "[0-3]\\d"},
		{"M", "1", "\\d{1,2}"},
		{"DDDD", "Monday", "[A-Z][a-z]{5,7}"},
		{"DDD", "Mon", "[A-Z][a-z]{2}"},
		{"DD", "02", "\\d{2}"},
		{"_D", "_2", "(?: \\d{1}|\\d{2})"},
		{"D", "2", "\\d{1,2}"},
		{"HH", "15", "\\d{2}"},
		{"hh", "03", "\\d{2}"},
		{"h", "3", "\\d{1,2}"},
		{"mm", "04", "\\d{2}"},
		{"m", "4", "\\d{1,2}"},
		{"ss", "05", "\\d{2}"},
		{"s", "5", "\\d{1,2}"},
		{".SSS", ".999999999", ".\\d{3,}"},
		{"P", "PM", "(?:am|AM|pm|PM)"},
		{"ZZZZZ", "-07:00", "[+-][0-9]{2}:[0-9]{2}"},
		{"ZZZZ", "-0700", "[+-][0-9]{4}"},
		{"ZZZ", "MST", "[A-Z]{3}"},
		{"ZZ", "Z07:00", "Z[0-9]{2}:[0-9]{2}"},
	}

	defParser = NewDefaultParser()
)

//===================== parser =====================

func Parse(data []byte) (time.Time, error) {
	tm, f := defParser.Parse(data)
	if f == nil {
		return tm, fmt.Errorf("Could not parse data %s, no such default format for it, or it is wrong date/time", data)
	}
	return tm, nil
}

func NewDefaultParser(usrFmts ...string) *parser {
	if len(usrFmts) == 0 {
		return NewParser(KnownFormats...)
	}

	dtFmts := make([]string, 0, len(KnownFormats)+len(usrFmts))
	dtFmts = append(dtFmts, usrFmts...)      // user defined/custom formats go first,
	dtFmts = append(dtFmts, KnownFormats...) // predefined/defaults formats go after
	return NewParser(dtFmts...)
}

// builds new parser with the give formats
func NewParser(fmts ...string) *parser {
	p := new(parser)
	p.formats = make([]*Format, len(fmts))

	for i, fmtStr := range fmts {
		noDate := !strings.ContainsAny(fmtStr, "YMD")
		hasYear := !noDate && strings.Contains(fmtStr, "Y")
		hasLocation := strings.Contains(fmtStr, "Z")
		dRegexp := regexp.MustCompile(fmt.Sprintf("(?P<%v>%v)", dateGroup, regexpMap(fmtStr)))

		grps := dRegexp.SubexpNames()
		if len(grps) < 2 || grps[1] != dateGroup { // grps[0] is a whole line
			panic(fmt.Sprintf("regular expression=%s doesn't have "+
				"required '%s' group", dRegexp, dateGroup))
		}

		p.formats[i] = &Format{
			frmt:        fmtStr,
			dLayout:     dateMap(fmtStr),
			dRegexp:     dRegexp,
			hasLocation: hasLocation,
			hasYear:     hasYear,
			noDate:      noDate,
		}
	}
	return p
}

// parses the given byte slice in accordance with the given formats
func (p *parser) Parse(buf []byte) (time.Time, *Format) {
	for _, dFmt := range p.formats {
		tm, err := dFmt.Parse(buf)
		if err == nil {
			return tm, dFmt
		}
	}

	//no suitable format found (bad and slow!), return empty time
	return time.Time{}, nil
}

// dateMap transforms human readable format (YYYY/MM/DD)
// to go-lang layout representation (2006/01/02), see terms table above
func dateMap(format string) string {
	layout := format
	for _, t := range terms {
		layout = strings.Replace(layout, t.format, t.layout, -1)
	}
	return layout
}

// regexpMap builds regular expression for a particular date format
// the resulting regexp can be used in order to locate the date in a line,
// see terms table above
func regexpMap(format string) string {
	re := format
	for _, t := range terms {
		re = strings.Replace(re, t.format, t.expr, -1)
	}
	return re
}

//===================== Format =====================

func (f *Format) Parse(buf []byte) (tm time.Time, err error) {
	match := f.dRegexp.FindSubmatch(buf)

	//match[0] - wholes string, match[1] - date group
	if len(match) < 2 {
		return tm, fmt.Errorf("no match")
	}

	// avoid byte[] slice copy and cast it directly to string,
	// careful (!) string is mutable through original byte slice
	str := *(*string)(unsafe.Pointer(&match[1]))

	if f.hasLocation {
		tm, err = time.Parse(f.dLayout, str)
	} else {
		tm, err = time.ParseInLocation(f.dLayout, str, time.UTC)
	}

	if err != nil {
		return tm, err
	}

	if f.noDate {
		tm = adjustDate(tm)
	} else if !f.hasYear {
		//no year given, trying to add one...
		tm = adjustYear(tm)
	}
	return tm, nil
}

func (f *Format) GetFormat() string {
	return f.frmt
}

func adjustYear(tm time.Time) time.Time {
	now := time.Now()
	year := now.Year()
	if tm.Month() > now.Month() {
		year--
	}

	return time.Date(year, tm.Month(), tm.Day(), tm.Hour(),
		tm.Minute(), tm.Second(), tm.Nanosecond(), tm.Location())
}

func adjustDate(tm time.Time) time.Time {
	y, M, d := time.Now().Date()
	h, m, s := tm.Clock()
	return time.Date(y, M, d, h, m, s, tm.Nanosecond(), tm.Location())
}
