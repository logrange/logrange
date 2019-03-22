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
	f, _ := NewFields(map[string]string{"name": "app12345", "pod": "a;kldfj;alkdjflakjdsflkajdf;lkadf;kl", "ip": "1.1.1.2"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Value("pod3")
		//strings.Index(string(f), "ip")
	}
}

func BenchmarkMapAccess(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewFieldsFromKVString("name=alkdjfl;akjdf;lkadjflajdfkl, c=12387491kjhkjhlkjhlkjhlkjhlkjhlkjhlkjhk41934")
		//kvstring.ToMap("name=alkdjfl;akjdf;lkadjflajdfkl, c=12387491kjhkjhlkjhlkjhlkjhlkjhlkjhlkjhk41934")
	}
}

func BenchmarkWriteKVS(b *testing.B) {
	fld, _ := NewFieldsFromKVString("aaa=bbbb,name=asdf1234123412341234")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fld.AsKVString()
	}
}

func TestFieldValue(t *testing.T) {
	testFieldValue(t, map[string]string{"aaa": "124"})
	testFieldValue(t, map[string]string{"aaa": "124", "b": "adsf"})
	testFieldValue(t, map[string]string{"aaa": ""})

	f, _ := NewFields(map[string]string{"aaa": "124", "b": "adsf"})
	if f.Value("b1") != "" {
		t.Fatal("fails")
	}
}

func TestNewFieldsFromKVString(t *testing.T) {
	testNewFieldsFromKVString(t, "", "")
	testNewFieldsFromKVString(t, "aa=bb", "\x02aa\x02bb")
	testNewFieldsFromKVString(t, `aa=bb, name="tttt\"t"`, "\x02aa\x02bb\x04name\x06tttt\"t")
}

func TestWriteAsKVS(t *testing.T) {
	testWriteAsKVS(t, "", "")
	testWriteAsKVS(t, "aa=1234", `aa=1234`)
	testWriteAsKVS(t, `{aa=1234, name = "a=,pp12"}`, `aa=1234,name="a=,pp12"`)
}

func testFieldValue(t *testing.T, mp map[string]string) {
	f, _ := NewFields(mp)
	for n, v := range mp {
		v1 := f.Value(n)
		if v1 != v {
			t.Fatal("expected ", v, " for the name=", n, ", but got ", v1, " ", len(f), []byte(f))
		}
	}
}

func testNewFieldsFromKVString(t *testing.T, kvs string, flds Fields) {
	fld, err := NewFieldsFromKVString(kvs)
	if err != nil {
		t.Fatal("uexpected error ", err)
	}
	if fld != flds {
		t.Fatal("expected ", flds, ", but got", fld)
	}
}

func testWriteAsKVS(t *testing.T, kvs, exp string) {
	fld, err := NewFieldsFromKVString(kvs)
	if err != nil {
		t.Fatal("uexpected error ", err)
	}
	res := fld.AsKVString()
	if res != exp {
		t.Fatal("expected ", exp, " but ", res)
	}
}
