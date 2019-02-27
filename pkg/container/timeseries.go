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

package container

import (
	"fmt"
	"time"
)

type (
	// Timeseries is a structure, which keeps time-series in the number of chained
	// buckets. This is a time-sliding window which can be used for smoothing
	// out an observed scalar value.
	Timeseries struct {
		// tail points to the last bucket, which points to head one etc.
		tail *ts_bucket

		// clockNow is the clock function. It used to get the current time
		clockNow TsClockNowF

		// bktDur is the bucket size in time duration
		bktDur time.Duration

		// tsDur is the time-series size in time duration
		tsDur time.Duration

		// newVal function returns new counted value (see TsValue)
		newVal TsNewValueF
		total  TsValue
	}

	// TsNewValueF a function which constructs a TsValue
	TsNewValueF func() TsValue

	// TsClockNowF a function whic returns current time
	TsClockNowF func() time.Time

	// TsValue is an interface which represents an immutable scalar value
	TsValue interface {
		Add(val TsValue) TsValue
		Sub(val TsValue) TsValue
	}

	// TsInt implements TsValue for int
	TsInt int

	ts_bucket struct {
		next  *ts_bucket
		sTime time.Time
		val   TsValue
	}
)

// NewTimeseries same as NewTimeseriesWithClock, but provides system time.Now()
// for discovering current time.
func NewTimeseries(bktDur, tsDur time.Duration, newValF TsNewValueF) *Timeseries {
	return NewTimeseriesWithClock(bktDur, tsDur, newValF, time.Now)
}

// NewTimeseriesWithClock constructs new Timeseries value. Expects to receive
// the bucket size(bktDur in time duration), the time-series in time duration
// in the tsDur parameter, the newValF allows to create new scalar values and
// the clck is a function which allows to discover current time
func NewTimeseriesWithClock(bktDur, tsDur time.Duration, newValF TsNewValueF, clck TsClockNowF) *Timeseries {
	if bktDur > tsDur || bktDur <= 0 {
		panic(fmt.Sprint("Wrong durations: both timeseries duration=", tsDur, " and bucket one=", bktDur, " must be positive, and the first one should be bigger then second one."))
	}
	ts := new(Timeseries)
	ts.tail = nil
	ts.clockNow = clck
	ts.bktDur = bktDur
	ts.tsDur = tsDur
	ts.newVal = newValF
	ts.total = newValF()

	ts.tail = new(ts_bucket)
	ts.tail.next = ts.tail
	ts.tail.val = newValF()
	ts.tail.sTime = clck().Truncate(bktDur)
	return ts
}

func (ts *Timeseries) Add(val TsValue) {
	now := ts.sweep()
	bkt := ts.getBucket(now)
	bkt.val = bkt.val.Add(val)
	ts.total = ts.total.Add(val)
}

func (ts *Timeseries) Total() TsValue {
	ts.sweep()
	return ts.total
}

func (ts *Timeseries) StartTime() time.Time {
	return ts.tail.next.sTime
}

func (ts *Timeseries) sweep() time.Time {
	now := ts.clockNow()
	if now.Sub(ts.tail.sTime) >= ts.tsDur {
		ts.total = ts.newVal()
		ts.tail.next = ts.tail
		ts.tail.val = ts.newVal()
		ts.tail.sTime = now.Truncate(ts.bktDur)
		return now
	}

	head := ts.tail.next
	for head != ts.tail && now.Sub(head.sTime) >= ts.tsDur {
		ts.total = ts.total.Sub(head.val)
		h := head.next
		head.next = nil
		head.val = nil
		head = h
		ts.tail.next = head
	}
	return now
}

func (ts *Timeseries) getBucket(now time.Time) *ts_bucket {
	d := now.Sub(ts.tail.sTime)
	if d < 0 {
		panic("don't support to add value in past")
	}

	if d < ts.bktDur {
		return ts.tail
	}

	newTail := new(ts_bucket)
	newTail.val = ts.newVal()
	newTail.next = ts.tail.next
	newTail.sTime = now.Truncate(ts.bktDur)
	ts.tail.next = newTail
	ts.tail = newTail
	return newTail
}

func NewTsInt() TsValue {
	return TsInt(0)
}

func (ti TsInt) Add(val TsValue) TsValue {
	return ti + val.(TsInt)
}

func (ti TsInt) Sub(val TsValue) TsValue {
	return ti - val.(TsInt)
}
