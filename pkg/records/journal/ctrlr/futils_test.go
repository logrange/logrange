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

package ctrlr

import (
	"testing"
)

func TestJournalPath(t *testing.T) {
	jp, err := journalPath("aaa", "123")
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}

	if jp != "aaa/23/123" {
		t.Fatal("Wrong dir=", jp)
	}

	jp, err = journalPath("aaa", "3")
	if err == nil {
		t.Fatal("err must be not nil, but err=nil")
	}
}
