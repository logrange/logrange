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

package lql

import (
	"fmt"
	"github.com/logrange/logrange/pkg/scanner/parser/date"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

// dateTimeParser allows to parse date-time formats allowed for the date-time points
// please see "github.com/logrange/logrange/pkg/scanner/parser/date"
var dateTimeParser = date.NewParser([]string{
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
	"YYYY-MM-DD HH:mm:ss.SSS ZZZZ",
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

	// todays time
	"HH:mm:ss.SSS ZZZZ",
	"HH:mm:ss ZZZZ",
	"HH:mm ZZZZ",
	"HH:mm:ss.SSS ZZZ",
	"HH:mm:ss ZZZ",
	"HH:mm ZZZ",
	"HH:mm:ss.SSS",
	"HH:mm:ss",
	"HH:mm",
}...)

// parseLqlDateTime allows to parse date-time in an extended format.
// The extended format allows to specify time in one of 3 forms:
// 		absolute: e.g. '2019-01-02 12:34:55'
// 		relative: e.g. '-3.5h' or  '-24m' etc.
//		special: e.g. 'week' or 'day' etc.
//
// The absolute form allows the date-time be specified in different
// formats, that accepts date, time or both. Known formats are provided
// in the dateTimeParser variable (see above)
//
// The relative form has the following format:
// -<number>(m|h|d]) where m stays for minutes, h for hour[s] and
// d for day[s] (24 hours)
// Examples are: '-1.5h' means the timestamp for 1 hour 30 mins ago from
// the current time
//
// there are following "special" are allowed:
// minute	- a minute ago
// hour 	- the time, when the hour starts
// day 		- the 12:00AM of today
// week 	- the timestamp of Sunday 12:00AM for the current week
func parseLqlDateTime(dt0 string) (time.Time, error) {
	dt := strings.ToLower(strings.Trim(dt0, " "))

	tm, err := parseRalativeDateTime(dt)
	if err == nil {
		return tm, nil
	}

	tm, err = parseConstantsDateTime(dt)
	if err == nil {
		return tm, nil
	}

	tm, fm := dateTimeParser.Parse(bytes.StringToByteArray(dt))
	if fm != nil {
		return tm, nil
	}

	v, err := strconv.ParseInt(dt, 10, 64)
	if err == nil {
		// well, use it like unix nano
		return time.Unix(0, v), nil
	}

	return time.Time{}, fmt.Errorf("could not parse value \"%s\" as relative or absolute timestamp.", dt0)
}

// parseRalativeDateTime parsing relative date-time
func parseRalativeDateTime(dt string) (time.Time, error) {
	if len(dt) == 0 || dt[0] != '-' {
		return time.Time{}, fmt.Errorf("wrong relative format. expecting -<number>(m|h|d), but got \"%s\"", dt)
	}

	dim := dt[len(dt)-1]
	var mult float64
	switch dim {
	case 'm':
		mult = float64(time.Minute)
	case 'h':
		mult = float64(time.Hour)
	case 'd':
		mult = float64(24 * time.Hour)
	default:
		return time.Time{}, fmt.Errorf("unknown dimension %c at %s", dim, dt)
	}

	val, err := strconv.ParseFloat(dt[1:len(dt)-1], 64)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "could not parse value %s", dt[1:len(dt)-1])
	}

	val = val * mult
	return time.Now().Add(-time.Duration(val)), nil
}

//parseConstantsDateTime allows to convert constant values to the time-point
func parseConstantsDateTime(dt string) (time.Time, error) {
	now := time.Now()
	switch dt {
	case "minute":
		_, _, s := now.Clock()
		now = now.Add(-time.Duration(s) * time.Second)
	case "hour":
		_, m, s := now.Clock()
		now = now.Add(
			-time.Duration(m)*time.Minute - time.Duration(s)*time.Second - time.Duration(now.Nanosecond()))
	case "day":
		h, m, s := now.Clock()
		now = now.Add(-time.Duration(h)*time.Hour - time.Duration(m)*time.Minute - time.Duration(s)*time.Second - time.Duration(now.Nanosecond()))
	case "week":
		h, m, s := now.Clock()
		wd := now.Weekday()
		h += 24 * int(wd)
		now = now.Add(-time.Duration(h)*time.Hour - time.Duration(m)*time.Minute - time.Duration(s)*time.Second - time.Duration(now.Nanosecond()))
	default:
		return time.Time{}, fmt.Errorf("unknown time constant \"%s\"", dt)
	}
	return now, nil
}
