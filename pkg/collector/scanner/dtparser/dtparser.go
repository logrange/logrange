package dtparser

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unsafe"
)

type (
	Parser struct {
		formats     []*ParserFormat
		defLocation *time.Location
	}

	ParserFormat struct {
		frmt        string
		goFrmt      string
		rExp        *regexp.Regexp
		hasLocation bool
		hasYear     bool
		parser      *Parser
	}

	// term structure describes transformation from user friendly symbol to
	// go-lang layout and part of regular expression that can be used
	// for finding the symbol in a text. Plese see terms slice below.
	term struct {
		symbol string
		layout string
		expr   string
	}
)

const (
	tsGroup = "timestamp"
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

	terms = []term{ //descending order of the 'alike' symbols is important
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

	ErrNotMatched = fmt.Errorf("not matched")
)

func NewDefaultParser() *Parser {
	return NewParser(KnownFormats)
}

func NewParser(frmts []string) *Parser {
	p := new(Parser)
	p.formats = make([]*ParserFormat, len(frmts))
	p.defLocation = time.UTC

	for i, frmt := range frmts {
		hasLocation := false
		if strings.Contains(frmt, "Z") {
			hasLocation = true
		}

		hasYear := false
		if strings.Contains(frmt, "Y") {
			hasYear = true
		}

		lineFmt := lmap(frmt)
		re := regexp.MustCompile(fmt.Sprintf("(?P<%v>%v)", tsGroup, emap(frmt)))
		grps := re.SubexpNames()
		if len(grps) < 2 || grps[1] != tsGroup {
			panic(fmt.Sprint("wrong regular expression ", lineFmt, " which doesn't have ", tsGroup, " group, or has many. grps=", grps, len(grps)))
		}

		p.formats[i] = &ParserFormat{
			frmt:        frmt,
			goFrmt:      lineFmt,
			rExp:        re,
			parser:      p,
			hasLocation: hasLocation,
			hasYear:     hasYear,
		}
	}
	return p
}

func (p *Parser) SetDefaultLocation(dl *time.Location) {
	p.defLocation = dl
}

func (p *Parser) Parse(buf []byte, ignorePF *ParserFormat) (time.Time, *ParserFormat) {
	for i, pf := range p.formats {
		if pf == ignorePF {
			continue
		}

		tm, err := pf.Parse(buf)
		if err == nil {
			if i > 0 {
				copy(p.formats[1:i+1], p.formats[:i])
				p.formats[0] = pf
			}
			return tm, pf
		}
	}
	return time.Time{}, nil
}

func (pf *ParserFormat) Parse(buf []byte) (tm time.Time, err error) {
	match := pf.rExp.FindSubmatch(buf)
	if len(match) < 2 {
		return tm, ErrNotMatched
	}
	str := *(*string)(unsafe.Pointer(&match[1]))

	if pf.hasLocation {
		tm, err = time.Parse(pf.goFrmt, str)
	} else {
		tm, err = time.ParseInLocation(pf.goFrmt, str, pf.parser.defLocation)
	}
	if err != nil {
		return tm, err
	}

	if !pf.hasYear {
		now := time.Now()
		year := now.Year()
		if tm.Month() > now.Month() {
			year--
		}
		tm = time.Date(year, tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond(), tm.Location())
	}
	return tm, nil
}

func (pf *ParserFormat) GetFormat() string {
	return pf.frmt
}

// lmap builds human readable fromat transformation to go-lang format representation
func lmap(format string) string {
	layout := format
	for _, t := range terms {
		layout = strings.Replace(layout, t.symbol, t.layout, -1)
	}
	return layout
}

func emap(format string) string {
	re := format
	for _, t := range terms {
		re = strings.Replace(re, t.symbol, t.expr, -1)
	}
	return re
}
