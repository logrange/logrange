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
	"github.com/logrange/logrange/pkg/utils/kvstring"
	"github.com/logrange/range/pkg/utils/bytes"
	"testing"
)

func BenchmarkLogEventParser(b *testing.B) {
	f, _ := NewFields(map[string]string{"name": "app12345", "pod2": "a;kldfj;alkdjflakjdsflkajdf;lkadf;kl", "ip12": "1.1.1.2"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.AsKVString()
		//f.Value("pod3")
		//m := make(map[string]string)
		//m["pod3"] = f.Value("ip12")
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

func BenchmarkMerge(b *testing.B) {
	fld := Parse("aaa=bbbb,name=asdf1234123412341234")
	m := map[string]string{"name": "app12345", "pod2": "a;kldfj;alkdjflakjdsflkajdf;lkadf;kl", "ip12": "1.1.1.2"}
	var w bytes.Writer
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fld.MergeWithMap(m, &w)
	}
}

func BenchmarkConcat(b *testing.B) {
	fld := Parse("aaa=bbbb,name=asdf1234123412341234")
	var w bytes.Writer
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fld.Concat(fld, &w)
	}
}

func BenchmarkNewFields(b *testing.B) {
	b.ReportAllocs()
	val := "test"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewFieldsFromSlice("stream", val)
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

func TestMerge(t *testing.T) {
	testMerge(t, "a=b", map[string]string{}, map[string]string{"a": "b"})
	testMerge(t, "a=bbb", map[string]string{"c": "d"}, map[string]string{"a": "bbb", "c": "d"})
	testMerge(t, "a=b", map[string]string{"a": "d"}, map[string]string{"a": "d"})
	testMerge(t, "a=b, b=c", map[string]string{"c": "d"}, map[string]string{"a": "b", "c": "d", "b": "c"})
}

func TestConcat(t *testing.T) {
	testConcat(t, "", "", "")
	testConcat(t, "", "c=d", "\x01c\x01d")
	testConcat(t, "a=b", "", "\x01a\x01b")
	testConcat(t, "a=b", "c=d", "\x01a\x01b\x01c\x01d")
	testConcat(t, "a=b,c=ddd", "c=d1", "\x01a\x01b\x01c\x03ddd\x01c\x02d1")
}

func TestNewFieldsFromSlice(t *testing.T) {
	f, err := NewFieldsFromSlice()
	if err != nil || f != "" {
		t.Fatal("Something goes wrong f=", " err=", err)
	}

	f, err = NewFieldsFromSlice("a", "b", "c")
	if err == nil {
		t.Fatal("Something goes wrong f=", " err=", err)
	}

	f, err = NewFieldsFromSlice("a", "b", "c", "d")
	if err != nil || f != "\x01a\x01b\x01c\x01d" {
		t.Fatal("Something goes wrong f=", " err=", err)
	}

	var longVal [257]byte
	_, err = NewFieldsFromSlice("a", string(longVal[:]))
	if err == nil {
		t.Fatal("Something goes wrong f=", " err=", err)
	}

	_, err = NewFieldsFromSlice(string(longVal[:]), "b")
	if err == nil {
		t.Fatal("Something goes wrong f=", " err=", err)
	}
}

func TestCheck(t *testing.T) {
	_, err := Check("abc")
	if err == nil {
		t.Fatal("Check must fail, but err=nil")
	}

	_, err = Check("\x03abc")
	if err != nil {
		t.Fatal("Check must not fail, but err=", err)
	}
}

func testMerge(t *testing.T, kvs string, m, res map[string]string) {
	flds := Parse(kvs)
	var w bytes.Writer
	flds2 := flds.MergeWithMap(m, &w)

	res2, err := kvstring.ToMap(flds2.AsKVString())
	if err != nil {
		t.Fatal("could not convert ", flds2.AsKVString(), " to map")
	}
	if !kvstring.MapsEquals(res2, res) {
		t.Fatal("res2=", res2, " is not equal to expected ", res)
	}
}

func testConcat(t *testing.T, kvs1, kvs2, res string) {
	f1 := Parse(kvs1)
	var w bytes.Writer
	f2 := Parse(kvs2)
	f := f1.Concat(f2, &w)
	if string(f) != res {
		t.Fatal("expected ", res, " but result is ", f)
	}
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
