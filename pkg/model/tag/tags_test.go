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

package tag

import (
	"reflect"
	"testing"
)

func TestRemoveCurlyBraces(t *testing.T) {
	testRemoveCurlyBraces(t, "aaa", "aaa", false)
	testRemoveCurlyBraces(t, "   aaa", "aaa", false)
	testRemoveCurlyBraces(t, "a  aa   ", "a  aa", false)
	testRemoveCurlyBraces(t, "   aaa  ", "aaa", false)
	testRemoveCurlyBraces(t, "{aaa}", "aaa", false)
	testRemoveCurlyBraces(t, "   {aaa}", "aaa", false)
	testRemoveCurlyBraces(t, "{a  aa}   ", "a  aa", false)
	testRemoveCurlyBraces(t, "   {aaa}  ", "aaa", false)
	testRemoveCurlyBraces(t, "{ {aaa}}", "aaa", false)
	testRemoveCurlyBraces(t, "   {{aaa} }", "aaa", false)
	testRemoveCurlyBraces(t, "{{a  aa}}   ", "a  aa", false)
	testRemoveCurlyBraces(t, "  { {aaa} } ", "aaa", false)
	testRemoveCurlyBraces(t, "{aaa", "", true)
	testRemoveCurlyBraces(t, "   aaa}", "", true)
	testRemoveCurlyBraces(t, "{a  aa} }   ", "", true)
	testRemoveCurlyBraces(t, "   {a{aa  ", "", true)

}

func TestSplitString(t *testing.T) {
	testsplitString(t, "", []string{""}, false)
	testsplitString(t, "=", []string{"", ""}, false)
	testsplitString(t, "=name", []string{"", "name"}, false)
	testsplitString(t, "name=", []string{"name", ""}, false)
	testsplitString(t, "name=app1", []string{"name", "app1"}, false)
	testsplitString(t, "name=app1, name=app2", []string{"name", "app1", " name", "app2"}, false)
	testsplitString(t, "name=ap\"p1,=\"p", []string{"name", "ap\"p1,=\"p"}, false)
	testsplitString(t, "name=ap\"p1,=p", []string{}, true)
	testsplitString(t, "\"name=app1,=p", []string{}, true)
}

func TestParseTags(t *testing.T) {
	testParseTags(t, "", emptyMap, false)
	testParseTags(t, "name=app", tagMap{"name": "app"}, false)
	testParseTags(t, " { name = app}", tagMap{"name": "app"}, false)
	testParseTags(t, " { name = \"app\"}", tagMap{"name": "app"}, false)
	testParseTags(t, " { name = \"a\\\\pp\"}", tagMap{"name": "a\\pp"}, false)
	testParseTags(t, " { name = app, t2=tt}", tagMap{"name": "app", "t2": "tt"}, false)
	testParseTags(t, " { name = app, t2=  \"tt\"}", tagMap{"name": "app", "t2": "tt"}, false)
	testParseTags(t, "name=", tagMap{"name": ""}, false)
	testParseTags(t, "name=-\"app\"", tagMap{"name": "-\"app\""}, false)
	testParseTags(t, "name-app", emptyMap, true)
	testParseTags(t, "name=-a\"pp", emptyMap, true)
}

func TestTagLine(t *testing.T) {
	testTagLine(t, "", "")
	testTagLine(t, "name=app", "name=app")
	testTagLine(t, "{name=app}", "name=app")
	testTagLine(t, "{ name=\"app\" }", "name=app")
	testTagLine(t, "{ name=\"a\\\"p\\\"p\" }", "name=a\"p\"p")
	testTagLine(t, "{ name=\"a\\\"pp\" }", "name=a\"pp")
	testTagLine(t, "{ name=\"app\"}", "name=app")
	testTagLine(t, "{ name=\"a==b\" }", "name=\"a==b\"")
	testTagLine(t, "{ name=\"a,b\" }", "name=\"a,b\"")
	testTagLine(t, "{ name=\"1.2.3.4\" }", "name=1.2.3.4")
	testTagLine(t, "name=app,bbb=cc", "bbb=cc,name=app")
	testTagLine(t, "name=,bbb=cc", "bbb=cc,name=\"\"")
	testTagLine(t, "name=app,   a=asdf, bbb=cc", "a=asdf,bbb=cc,name=app")
	testTagLine(t, "name=\"app,   a=asdf\", bbb=cc", "bbb=cc,name=\"app,   a=asdf\"")
}

func TestSet(t *testing.T) {
	set, err := Parse(`{name=app1, ip="1.2"}`)
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if set.Line() != `ip=1.2,name=app1` {
		t.Fatal("expected Line ip=1.2,name=app1 but ", set.Line())
	}
	if v := set.Tag("ip"); v != "1.2" {
		t.Fatal("wrong tag value ", set)
	}
}

func TestSubset(t *testing.T) {
	s1, _ := Parse(`a=1,b=2`)
	s2, _ := Parse(`a=1,b=2, c=3`)
	s3, _ := Parse(`b=2,c=3`)
	s4, _ := Parse(`c=3,b=2`)
	if !s1.SubsetOf(s2) ||
		s2.SubsetOf(s1) ||
		!s4.SubsetOf(s2) ||
		s1.SubsetOf(s4) ||
		!s3.SubsetOf(s4) ||
		!s4.SubsetOf(s3) ||
		!s3.SubsetOf(s3) {
		t.Fatal("subset test failed")
	}
}

func testTagLine(t *testing.T, tags string, line Line) {
	tm, err := parseTags(tags)
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if tm.line() != line {
		t.Fatal("Expected ", line, " but received ", tm.line())
	}
}

func testParseTags(t *testing.T, tags string, expm tagMap, errOk bool) {
	m, err := parseTags(tags)
	if err != nil {
		if errOk {
			return
		}
		t.Fatal("unexpected err=", err)
	}

	if errOk {
		t.Fatal("must be an error, but it is nil")
	}

	if !m.equalTo(expm) {
		t.Fatal("expected ", expm, " but returned", m)
	}
}

func testsplitString(t *testing.T, in string, exp []string, errOk bool) {
	res, err := splitString(in, '=', ',', []string{})
	if err != nil {
		if errOk {
			return
		}
		t.Fatal("unexpected err=", err)
	}

	if errOk {
		t.Fatal("must be an error, but it is nil")
	}

	if !reflect.DeepEqual(res, exp) {
		t.Fatal(" expected ", exp, " but the result is ", res, " reslen=", len(res))
	}
}

func testRemoveCurlyBraces(t *testing.T, in, out string, errOk bool) {
	out2, err := removeCurlyBraces(in)
	if err != nil {
		if !errOk {
			t.Fatal("Must not be error, but err=", err)
		}
		return
	}

	if errOk {
		t.Fatal("must be an error, but it is nil ", in)
	}

	if out != out2 {
		t.Fatal("expected ", out, ", but got ", out2)
	}
}
