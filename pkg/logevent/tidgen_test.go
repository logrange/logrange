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

package logevent

import (
	"strconv"
	"testing"
)

func BenchmarkTagIdGenerator(b *testing.B) {
	tig := NewTagIdGenerator(10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tgs := strconv.FormatInt(int64(i), 10)
		tig.GetOrCreateId(tgs)
	}
}

func TestGetOrCreate(t *testing.T) {
	tig := NewTagIdGenerator(2)
	ti1 := tig.GetOrCreateId("a")
	ti2 := tig.GetOrCreateId("b")
	if ti1 == ti2 || ti1 != tig.GetOrCreateId("a") || ti2 != tig.GetOrCreateId("b") {
		t.Fatal("something goes wrong")
	}

	ti3 := tig.GetOrCreateId("c")
	if ti3 <= ti2 || ti1 == tig.GetOrCreateId("a") || ti3 != tig.GetOrCreateId("c") {
		t.Fatal("something goes wrong ti3=", ti3, ", ti2=", ti2, ", ti1=", ti1, " ", tig.GetOrCreateId("a"), " ", tig.GetOrCreateId("b"))
	}

	if tig.knwnIds.Len() != 2 || len(tig.ids) != 2 {
		t.Fatal("lengths must be 2")
	}
}

func TestNewTagId(t *testing.T) {
	tid1 := newTagId()
	tid2 := newTagId()
	if tid2 != tid1+0x10000 {
		t.Fatal("Expecting tid2=", tid1+0x10000, ", but tid1=", tid1, " and the tid2=", tid2)
	}
}
