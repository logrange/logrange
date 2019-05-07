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
	"context"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"io"
)

type (
	// Iterator interface provides methods for iterating over a collection of LogEvents
	Iterator interface {
		// Next switches to the next event, if any
		Next(ctx context.Context)

		// Get returns current LogEvent, the TagsCond for the event or an error if any. It returns io.EOF when end of the collection is reached
		Get(ctx context.Context) (LogEvent, tag.Line, error)

		// Release allows to free underlying resources if they were used for the iteration.
		// This method doesn't affect the iterator position, so subsequent calls to Next() or Get() will
		// get the same result if the Release was not invoked
		Release()

		// SetBackward allows to change direction to backward (true) or forward (false)
		SetBackward(bool)

		// CurrentPos returns the current iterator position
		CurrentPos() records.IteratorPos
	}

	// LogEventIterator struct wraps a records.Iterator and provides LogEvent Iterator interface over it.
	LogEventIterator struct {
		tags tag.Line
		it   records.Iterator
		st   int
		le   LogEvent
	}
)

// Wrap sets the underlying records.Iterator it
func (lei *LogEventIterator) Wrap(tags tag.Line, it records.Iterator) *LogEventIterator {
	lei.tags = tags
	lei.it = it
	lei.st = 0
	return lei
}

// Next switches to the next LogEvent record
func (lei *LogEventIterator) Next(ctx context.Context) {
	lei.it.Next(ctx)
	lei.le.Release()
	lei.st = 0
}

// Get returns current LogEvent record
func (lei *LogEventIterator) Get(ctx context.Context) (LogEvent, tag.Line, error) {
	if lei.st == 1 {
		return lei.le, lei.tags, nil
	}

	rec, err := lei.it.Get(ctx)
	if err == nil {
		_, err = lei.le.Unmarshal(rec, false)

	}
	return lei.le, lei.tags, err
}

func (lei *LogEventIterator) Release() {
	if lei.st != 0 {
		lei.le.MakeItSafe()
	}
	lei.it.Release()
}

func (lei *LogEventIterator) SetBackward(bkwd bool) {
	lei.it.SetBackward(bkwd)
}

func (lei *LogEventIterator) CurrentPos() records.IteratorPos {
	return lei.it.CurrentPos()
}

type TestLogEventsWrapper struct {
	les  []LogEvent
	idx  int
	bkwd bool
}

func NewTestLogEventsWrapper(les []LogEvent) *TestLogEventsWrapper {
	return &TestLogEventsWrapper{les, 0, false}
}

func (tle *TestLogEventsWrapper) Next(ctx context.Context) {
	if tle.bkwd {
		if tle.idx >= 0 {
			tle.idx--
		}
		return
	}

	if tle.idx < len(tle.les) {
		tle.idx++
	}
}

func (tle *TestLogEventsWrapper) Get(ctx context.Context) (records.Record, error) {
	if tle.bkwd && tle.idx >= len(tle.les) {
		tle.idx = len(tle.les) - 1
	}

	if !tle.bkwd && tle.idx < 0 {
		tle.idx = 0
	}

	if tle.idx < len(tle.les) && tle.idx >= 0 {
		buf := make([]byte, tle.les[tle.idx].WritableSize())
		tle.les[tle.idx].Marshal(buf)
		return buf, nil
	}

	return nil, io.EOF
}

func (tle *TestLogEventsWrapper) Release() {
}

func (tle *TestLogEventsWrapper) SetBackward(bkwd bool) {
	tle.bkwd = bkwd
}

func (tle *TestLogEventsWrapper) CurrentPos() records.IteratorPos {
	return tle.idx
}
