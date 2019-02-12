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
package container

import (
	"testing"
	"time"
)

func TestTsInt(t *testing.T) {
	i := TsInt(10)
	if i.Add(TsInt(20)) != TsInt(TsInt(30)) || i.Sub(TsInt(20)) != TsInt(-10) || i != TsInt(10) {
		t.Fatal("Something goes wrong with Add or Sub for TsInt")
	}
}

func TestTsIncremental(t *testing.T) {
	bktSize := time.Second
	tsSize := 3 * bktSize
	st := time.Now()
	clck := func() time.Time {
		return st
	}

	ts := NewTimeseriesWithClock(bktSize, tsSize, NewTsInt, clck)
	if ts.Total().(TsInt) != TsInt(0) {
		t.Fatal("Should be 0!")
	}
	st = st.Add(time.Millisecond)

	ts.Add(TsInt(1))
	if ts.Total().(TsInt) != TsInt(1) || ts.tail.next != ts.tail {
		t.Fatal("Should be 1!")
	}
	st = st.Add(time.Second)

	ts.Add(TsInt(1))
	if ts.Total().(TsInt) != TsInt(2) || ts.tail.next == ts.tail {
		t.Fatal("Should be 2!")
	}
	st = st.Add(time.Second)

	ts.Add(TsInt(1))
	if ts.Total().(TsInt) != TsInt(3) {
		t.Fatal("Should be 3!")
	}
	st = st.Add(time.Second)

	ts.Add(TsInt(1))
	if ts.Total().(TsInt) != TsInt(3) {
		t.Fatal("Should be 3!")
	}

	st = st.Add(5 * time.Second)
	if ts.Total().(TsInt) != TsInt(0) || ts.tail.next != ts.tail {
		t.Fatal("Should be 0!")
	}
}

func TestTsSameBucket(t *testing.T) {
	bktSize := time.Second
	tsSize := 2 * bktSize
	st := time.Now()
	clck := func() time.Time {
		return st
	}

	ts := NewTimeseriesWithClock(bktSize, tsSize, NewTsInt, clck)
	if ts.Total().(TsInt) != TsInt(0) {
		t.Fatal("Should be 0!")
	}
	st = st.Add(10 * time.Millisecond)

	ts.Add(TsInt(1))
	ts.Add(TsInt(2))
	ts.Add(TsInt(3))

	if ts.Total().(TsInt) != TsInt(6) || ts.tail.next != ts.tail {
		t.Fatal("Should be 6!")
	}

	st = st.Add(time.Second)
	if ts.Total().(TsInt) != TsInt(6) || ts.tail.next != ts.tail {
		t.Fatal("Should be 6!")
	}

	ts.Add(TsInt(-1))
	if ts.Total().(TsInt) != TsInt(5) || ts.tail.next == ts.tail {
		t.Fatal("Should be 5!")
	}

	st = st.Add(time.Second)
	if ts.Total().(TsInt) != TsInt(-1) || ts.tail.next != ts.tail {
		t.Fatal("Should be -1!")
	}

	st = st.Add(time.Second)
	if ts.Total().(TsInt) != TsInt(0) || ts.tail.next != ts.tail {
		t.Fatal("Should be 0!")
	}
}
