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

package model

import (
	"context"
	"github.com/logrange/range/pkg/records"
	"io"
)

type (
	// Iterator interface provides methods for iterating over a collection of LogEvents
	Iterator interface {
		// Next switches to the next event, if any
		Next(ctx context.Context)

		// Get returns current LogEvent, the Tags for the event or an error if any. It returns io.EOF when end of the collection is reached
		Get(ctx context.Context) (LogEvent, TagLine, error)
	}

	// LogEventIterator struct wraps a records.Iterator and provides LogEvent Iterator interface over it.
	LogEventIterator struct {
		tags TagLine
		it   records.Iterator
		st   int
		le   LogEvent
	}
)

// Wrap sets the underlying records.Iterator it
func (lei *LogEventIterator) Wrap(tags TagLine, it records.Iterator) *LogEventIterator {
	lei.tags = tags
	lei.it = it
	lei.st = 0
	return lei
}

// Next switches to the next LogEvent record
func (lei *LogEventIterator) Next(ctx context.Context) {
	lei.it.Next(ctx)
	lei.st = 0
}

// Get returns current LogEvent record
func (lei *LogEventIterator) Get(ctx context.Context) (LogEvent, TagLine, error) {
	if lei.st == 1 {
		return lei.le, lei.tags, nil
	}

	rec, err := lei.it.Get(ctx)
	if err == nil {
		_, err = lei.le.Unmarshal(rec, false)

	}
	return lei.le, lei.tags, err
}

type testLogEventsWrapper struct {
	les []LogEvent
	idx int
}

func newTestLogEventsWrapper(les []LogEvent) *testLogEventsWrapper {
	return &testLogEventsWrapper{les, 0}
}

func (tle *testLogEventsWrapper) Next(ctx context.Context) {
	if tle.idx < len(tle.les) {
		tle.idx++
	}
}

func (tle *testLogEventsWrapper) Get(ctx context.Context) (records.Record, error) {
	if tle.idx < len(tle.les) {
		buf := make([]byte, tle.les[tle.idx].WritableSize())
		tle.les[tle.idx].Marshal(buf)
		return buf, nil
	}
	return nil, io.EOF
}
