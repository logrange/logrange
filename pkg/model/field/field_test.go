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

package field

import (
	"testing"
)

func BenchmarkLogEventParser(b *testing.B) {
	f := newFieldFromMap(map[string]string{"name": "app12345", "pod": "a;kldfj;alkdjflakjdsflkajdf;lkadf;kl", "ip": "1.1.1.2"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Value("ip1")
		//strings.Index(string(f), "ip")
	}
}

func BenchmarkMapAccess(b *testing.B) {
	f := map[string]string{"name": "app12345", "pod": "a;kldfj;alkdjflakjdsflkajdf;lkadf;kl", "ip": "1.1.1.2"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f["ip1"]
		//strings.Index(string(f), "ip")
	}
}

func TestFieldValue(t *testing.T) {
	testFieldValue(t, map[string]string{"aaa": "124"})
	testFieldValue(t, map[string]string{"aaa": "124", "b": "adsf"})
	testFieldValue(t, map[string]string{"aaa": ""})

	f := newFieldFromMap(map[string]string{"aaa": "124", "b": "adsf"})
	if f.Value("b1") != "" {
		t.Fatal("fails")
	}
}

func testFieldValue(t *testing.T, mp map[string]string) {
	f := newFieldFromMap(mp)
	for n, v := range mp {
		v1 := f.Value(n)
		if v1 != v {
			t.Fatal("expected ", v, " for the name=", n, ", but got ", v1, " ", len(f), []byte(f))
		}
	}
}
