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

package cursor

import (
	"context"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"io"
	"reflect"
	"testing"
)

type testLogEventsWrapper struct {
	les []model.LogEvent
	idx int
}

func newTestLogEventsWrapper(les []model.LogEvent) *testLogEventsWrapper {
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

func (tle *testLogEventsWrapper) Release() {

}

func TestFilter(t *testing.T) {
	les := []model.LogEvent{{1, []byte("asdfasdf"), ""}, {2, []byte("as2df"), ""}, {3, []byte("asd3f"), ""}, {4, []byte("jjjj"), ""},
		{5, []byte("jjjjee"), ""}}
	lew := (&model.LogEventIterator{}).Wrap(tag.Line("aaa=bbb"), newTestLogEventsWrapper(les))

	exp, err := lql.ParseExpr("ts = 4 OR msg contains 'asdf'")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	fit, err := newFIterator(lew, exp)
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	le, _, err := fit.Get(nil)
	if !reflect.DeepEqual(le, les[0]) {
		t.Fatal("Expected ", les[0], " but received ", le)
	}

	fit.Next(nil)
	le, _, err = fit.Get(nil)
	if !reflect.DeepEqual(le, les[3]) {
		t.Fatal("Expected ", les[3], " but received ", le)
	}

	fit.Next(nil)
	le, _, err = fit.Get(nil)
	if err != io.EOF {
		t.Fatal("Expected err==io.EOR, but err=", err, " le=", le)
	}
}
