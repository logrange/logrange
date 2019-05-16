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
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/utils"
	"testing"
)

func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseLql("SELECT SOURCE a>b WHERE 'from'='this is tag value' or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	}
}

func TestParse(t *testing.T) {
	testOk(t, `select "all"`)
	testOk(t, "select limit 100")
	testOk(t, `select "all" limit 100`)
	testOk(t, "select offset 123 ")
	testOk(t, "select 'format-%ts-%pod' limit 100")
	testOk(t, "select 'format-%ts-%pod' position tail limit 100")
	testOk(t, "select 'format-%ts-%pod' position 'head' limit 100")
	testOk(t, "select position head limit 100")
	testOk(t, "select position asdf limit 100")
	testOk(t, "select position 'hasdf123' limit 100")
	testOk(t, "select WHERE NOT a='1234' limit 100")
	testOk(t, "select WHERE NOT (a=\"12\\\\'34\" AND c=abc) limit 100")
	testOk(t, "select WHERE NOT a='1234' AND c=abc limit 100")
	testOk(t, "select WHERE NOT a='1234' AND not c=abc limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or x=123 limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not x=123 limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not (x=123) limit 100")
	testOk(t, "select WHERE (NOT (a='1234' AND c=abc)) or not (x=123 or c = abc) limit 100")
	testOk(t, "select WHERE a='1234' AND bbb>=adfadf234798 or xxx = yyy limit 100")
	testOk(t, "select WHERE a='1234' AND bbb like 'adfadf234798*' or xxx = yyy limit 10")
	testOk(t, "SELECT from a=b OR b contains 'r' WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, "SELECT From a=b AND c=d WHERE filename=\"system.log\" or filename=\"wifi.log\" OFFSET 0 LIMIT -1")
	testOk(t, `SELECT FROM a>b WHERE from='this is tag value' or filename="wifi.log" OFFSET 0 LIMIT -1`)
	testOk(t, `SELECT Range "2019-03-11 12:34:43"`)
	testOk(t, `SELECT Range "-1.0m"`)
	testOk(t, `SELECT Range [:"-1.0m"]`)
	testOk(t, `SELECT Range ["minute":"-1.0m"]`)
	testOk(t, `show PARTITIONS`)
	testOk(t, `SHOW PARTITIONs from1="abc"`)
	testOk(t, `SHOW PARTITIONs from1="abc" offset 10`)
	testOk(t, `SHOW PARTITIONs from1="abc" offset 10 limit 1`)
	testOk(t, `SHOW PARTITIONs offset 10 limit 1`)
	testOk(t, `SHOW PARTITIONs limit 1`)
	testOk(t, `SHOW pipes`)
	testOk(t, `SHOW Pipes offset 10 limit 1`)
	testOk(t, `SHOW Pipes offset 10`)
	testOk(t, `SHOW Pipes limit 1`)
	testOk(t, `create Pipe asb`)
	testOk(t, `create Pipe aaa from {a=1,b=2}`)
	testOk(t, `create Pipe aaa from a=1 or b=2 where ts=1`)
	testOk(t, `create Pipe aaa where ts=1`)
	testOk(t, "describe partition {fff=aaa}")
	testOk(t, "describe partition {file=anme,c=d}")
	testOk(t, "describe pipe aaa")
	testOk(t, "delete pipe aaa")
	testOk(t, "truncate")
	testOk(t, "truncate {fff=aaa}")
	testOk(t, "truncate file=anme AND c=d minsize 3G maxsize 20 ")
	testOk(t, "truncate dryrun {fff=aaa} before '2019-03-11 12:34:43'")
	testOk(t, "truncate dryrun {fff=aaa} before '2019-03-11 12:34:43' maxdbsize 13G")
	testOk(t, "truncate dryrun maxdbsize 13G")
}

func TestParams(t *testing.T) {
	l := testOk(t, "Select 'abc' where a = '123' position tail offset -10 limit 13")
	if utils.GetStringVal(l.Select.Format, "") != "abc" || l.Select.Position.PosId != "tail" || *l.Select.Offset != -10 || *l.Select.Limit != 13 {
		t.Fatal("Something goes wrong ", l.Select)
	}
}

func TestPosition(t *testing.T) {
	l := testOk(t, "Select 'abc' where a = '123' position 'tail' offset -10 limit 13")
	if l.Select.Position.PosId != "tail" {
		t.Fatal("Something goes wrong ", l.Select)
	}

	l = testOk(t, "Select 'abc' where a = '123' position tail offset -10 limit 13")
	if l.Select.Position.PosId != "tail" {
		t.Fatal("Something goes wrong ", l.Select)
	}

	posId := "AAAABXNyY0lkAAAE0gAAAAAAAeIqAAAAGHNyYzEyMzQ3OUAkJV8gQTIzNEF6cUlkMgAAAA4AAAAAAAAE0g=="
	l = testOk(t, "Select 'abc' where a = '123' position '"+posId+"' offset -10 limit 13")
	if l.Select.Position.PosId != posId {
		t.Fatal("Something goes wrong ", l.Select)
	}
}

func TestParseTagsSource(t *testing.T) {
	testParseSource(t, `{ asdfd="sf ,\\=df" , d=d }`, true, false)
	testParseSource(t, `{asdfd="sf,\\=df",c="",b=12\34.1234.1324.1234,d=asdf}`, true, false)
	testParseSource(t, `a = b and c like 'asdf*'`, false, false)
}

func TestParseWhere(t *testing.T) {
	testWhereOk(t, "a=adsf and b=adsf")
}

func TestConditionString(t *testing.T) {
	testCondParse(t, "a like 123")
	testCondParse(t, `a like '12"3'`)
	testCondParse(t, `a=b`)
	testCondParse(t, `a=bcd`)
}

func TestParsingRange(t *testing.T) {
	r := testParsingRange(t, `range "-123.3M"`)
	if r.TmPoint1 == nil || r.TmPoint2 != nil {
		t.Fatal("expecting only tmPoint1, but ", r)
	}

	r = testParsingRange(t, `range [:"-123.3M"]`)
	if r.TmPoint1 != nil || r.TmPoint2 == nil {
		t.Fatal("expecting only tmPoint2, but ", r)
	}

	r = testParsingRange(t, `range ["-23H":"-123.3M"]`)
	if r.TmPoint1 == nil || r.TmPoint2 == nil {
		t.Fatal("expecting only tmPoint2, but ", r)
	}
}

func testParsingRange(t *testing.T, sel string) *Range {
	var s Select
	err := parserSelect.ParseString(sel, &s)
	if err != nil || s.Range == nil {
		t.Fatal("Expecting range and no error, but err=", err)
	}
	return s.Range
}

func testCondParse(t *testing.T, str string) {
	var c, c2 Condition
	err := parserCondition.ParseString(str, &c)
	if err != nil {
		t.Fatal("err=", err)
	}
	err = parserCondition.ParseString(c.String(), &c2)
	if err != nil {
		t.Fatal("could not parse ", c.String(), " err=", err)
	}
	if c.String() != c2.String() {
		t.Fatal("something goes wrong ", &c, &c2)
	}
}

func testParseSource(t *testing.T, exp string, tags, errOk bool) {
	s, err := ParseSource(exp)
	if err == nil && errOk || err != nil && !errOk {
		t.Fatal("Wrong source ", exp, ", err=", err, "errOk=", errOk)
	}
	if !tags {
		return
	}

	if s == nil || s.Tags == nil {
		t.Fatal("Tags must be present")
	}
	tgs, err := tag.Parse(exp)
	if err != nil || !tgs.Equals(s.Tags.Tags) {
		t.Fatal("Tags must be equal, but in=", tgs, ", parsed=", s.Tags.Tags)
	}
}

func testWhereOk(t *testing.T, whr string) *Expression {
	e, err := ParseExpr(whr)
	if err != nil {
		t.Fatal("whr=\"", whr, "\" unexpected err=", err)
	}
	return e
}

func testOk(t *testing.T, lql string) *Lql {
	l, err := ParseLql(lql)
	if err != nil {
		t.Fatal("lql=\"", lql, "\" unexpected err=", err)
	}

	l2, err := ParseLql(l.String())
	if err != nil {
		t.Fatal("Initial lql=", lql, ", l.String()=\"", l.String(), "\" unexpected err=", err)
	}

	if l.String() != l2.String() {
		t.Fatal("Initial lql=", lql, ", l.String()=", l.String(), ", l2.String()=", l2.String())
	}

	return l
}
