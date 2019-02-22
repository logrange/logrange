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
	"io"
	"reflect"
	"testing"
)

func TestLogEventIterator(t *testing.T) {
	les := []LogEvent{{1, "asdfasdf"}, {2, "asdf"}, {3, "asdf"}}
	lew := newTestLogEventsWrapper(les)

	lei := &LogEventIterator{}
	lei.Wrap(lew)
	idx := 0
	for {
		le, err := lei.Get(nil)
		if err == io.EOF {
			break
		}
		le2, err := lei.Get(nil)
		if !reflect.DeepEqual(le, le2) {
			t.Fatal("expecting le=", le, " to be equal to ", le2)
		}
		if !reflect.DeepEqual(le, les[idx]) {
			t.Fatal("expected ", les[idx], ", but got ", le)
		}
		idx++
		lei.Next(nil)
	}
	if idx != 3 {
		t.Fatal("Must be 3, but idx=", idx)
	}
}
