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

package lql

import (
	"testing"
)

func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse("SELECT SOURCE a>b WHERE 'from'='this is tag value' or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	}
}

func TestParse(t *testing.T) {
	testOk(t, "select limit 120")
	testOk(t, "select limit 100")
	testOk(t, "select offset 123 limit 100")
	testOk(t, "select format 'format-%ts-%pod' limit 100")
	testOk(t, "select format 'format-%ts-%pod' position tail limit 100")
	testOk(t, "select format 'format-%ts-%pod' position 'head' limit 100")
	testOk(t, "select position head limit 100")
	testOk(t, "select position asdf limit 100")
	testOk(t, "select position 'hasdf123' limit 100")
	testOk(t, "select WHERE NOT a='1234' limit 100")
	testOk(t, "select WHERE NOT (a='1234' AND c=abc) limit 100")
	testOk(t, "select WHERE NOT a='1234' AND c=abc limit 100")
	testOk(t, "select WHERE NOT a='1234' AND not c=abc limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or x=123 limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not x=123 limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not (x=123) limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not (x=123 or c = abc) limit 100")
	testOk(t, "select WHERE a='1234' AND bbb>=adfadf234798* or xxx = yyy limit 100")
	testOk(t, "select WHERE a='1234' AND bbb like 'adfadf234798*' or xxx = yyy limit 10")
	testOk(t, "SELECT source a=b OR b contains 'r' WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, "SELECT Source a=b AND c=d WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, "SELECT SOURCE a>b WHERE 'from'='this is tag value' or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
}

func TestParams(t *testing.T) {
	s := testOk(t, "Select format 'abc' where a = '123' position tail offset -10 limit 13")
	if s.Format != "abc" || s.Position.PosId != "tail" || *s.Offset != -10 || s.Limit != 13 {
		t.Fatal("Something goes wrong ", s)
	}
}

func TestPosition(t *testing.T) {
	s := testOk(t, "Select format 'abc' where a = '123' position 'tail' offset -10 limit 13")
	if s.Position.PosId != "tail" {
		t.Fatal("Something goes wrong ", s)
	}

	s = testOk(t, "Select format 'abc' where a = '123' position tail offset -10 limit 13")
	if s.Position.PosId != "tail" {
		t.Fatal("Something goes wrong ", s)
	}

	posId := "AAAABXNyY0lkAAAE0gAAAAAAAeIqAAAAGHNyYzEyMzQ3OUAkJV8gQTIzNEF6cUlkMgAAAA4AAAAAAAAE0g=="
	s = testOk(t, "Select format 'abc' where a = '123' position '"+posId+"' offset -10 limit 13")
	if s.Position.PosId != posId {
		t.Fatal("Something goes wrong ", s)
	}
}

func TestUnquote(t *testing.T) {
	testUnquote(t, "'aaa", "'aaa")
	testUnquote(t, "'aaa\"", "'aaa\"")
	testUnquote(t, "aaa'", "aaa'")
	testUnquote(t, "\"aaa'", "\"aaa'")
	testUnquote(t, "   'aaa'", "aaa")
	testUnquote(t, "\"aaa\"   ", "aaa")
	testUnquote(t, "   'aaa\"", "   'aaa\"")
	testUnquote(t, " aaa   ", " aaa   ")
}

func testUnquote(t *testing.T, in, out string) {
	if unquote(in) != out {
		t.Fatal("unqoute(", in, ") != ", out)
	}
}

func TestParseWhere(t *testing.T) {
	testWhereOk(t, "a=adsf and b=adsf")
}

func testWhereOk(t *testing.T, whr string) *Expression {
	e, err := ParseExpr(whr)
	if err != nil {
		t.Fatal("whr=\"", whr, "\" unexpected err=", err)
	}
	return e
}

func testOk(t *testing.T, kql string) *Select {
	s, err := Parse(kql)
	if err != nil {
		t.Fatal("kql=\"", kql, "\" unexpected err=", err)
	}
	t.Log(s.Limit)
	return s
}
