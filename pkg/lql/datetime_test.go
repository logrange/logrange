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
	"math"
	"testing"
	"time"
)

func TestParseConstantsDateTime(t *testing.T) {
	words := []string{"minute", "hour", "day", "week"}
	for _, w := range words {
		tm, err := parseConstantsDateTime(w)
		if err != nil {
			t.Fatal("expecting no error, but err=", err)
		}
		fmt.Println(tm)
	}
}

func TestParseRalativeDateTime(t *testing.T) {
	_, err := parseRalativeDateTime(" - 1234 H")
	if err == nil {
		t.Fatal("must be error, but it is not")
	}

	_, err = parseRalativeDateTime(" -1234 H")
	if err == nil {
		t.Fatal("must be error, but it is not")
	}

	testParseRalativeDateTime(t, "-0.01m", time.Second)
	testParseRalativeDateTime(t, "-1m", time.Second+time.Minute)
	testParseRalativeDateTime(t, "-1.9m", time.Minute*2)
	testParseRalativeDateTime(t, "-2.1m", time.Minute*3)
	testParseRalativeDateTime(t, "-1.1h", time.Hour+10*time.Minute)
	testParseRalativeDateTime(t, "-5.5d", time.Hour*12*12)
}

func TestParseLqlDateTime(t *testing.T) {
	testParseLqlDateTime(t, " minute ", time.Minute*2, false)
	testParseLqlDateTime(t, " HOUR ", time.Hour*2, false)
	testParseLqlDateTime(t, "Day", time.Hour*25, false)
	testParseLqlDateTime(t, "22:25 -0700", time.Hour*25, true)
	testParseLqlDateTime(t, "22:25 +0700", time.Hour*25, true)
	testParseLqlDateTime(t, "22:25:34.534 -0700", time.Hour*25, true)
	testParseLqlDateTime(t, "2019-01-01 22:25:34 -0700", time.Duration(math.MaxInt64/2), true)
}

func testParseLqlDateTime(t *testing.T, dt string, dur time.Duration, futureOk bool) {
	now := time.Now()
	tm, err := parseLqlDateTime(dt)
	if err != nil {
		t.Fatal("expecting no error, but err=", err, " for dt=", dt, " duration ", dur)
	}

	if (!futureOk && tm.Sub(now) > 0) || now.Sub(tm) > dur {
		t.Fatal("expecting tm=", tm, " be less than ", dur, " of now=", now)
	}

	fmt.Println("now=", now, " tm=", tm)
}

func testParseRalativeDateTime(t *testing.T, dt string, dur time.Duration) {
	now := time.Now()
	tm, err := parseRalativeDateTime(dt)
	if err != nil {
		t.Fatal("no error expected, but err=", err, " for ", dt, " with the time duration ", dur)
	}
	if tm.Sub(now) > 0 || now.Sub(tm) > dur {
		t.Fatal("something wrong with tm=", tm, " should be around of ", now, " and not less than duration ", dur)
	}
}
