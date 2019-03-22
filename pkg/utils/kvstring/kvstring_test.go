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

package kvstring

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

func TestToMap(t *testing.T) {
	testParseTags(t, "", map[string]string{}, false)
	testParseTags(t, "name=app", map[string]string{"name": "app"}, false)
	testParseTags(t, " { name = app}", map[string]string{"name": "app"}, false)
	testParseTags(t, " { name = \"app\"}", map[string]string{"name": "app"}, false)
	testParseTags(t, " { name = \"a\\\\pp\"}", map[string]string{"name": "a\\pp"}, false)
	testParseTags(t, " { name = app, t2=tt}", map[string]string{"name": "app", "t2": "tt"}, false)
	testParseTags(t, " { name = app, t2=  \"tt\"}", map[string]string{"name": "app", "t2": "tt"}, false)
	testParseTags(t, "name=", map[string]string{"name": ""}, false)
	testParseTags(t, "name=-\"app\"", map[string]string{"name": "-\"app\""}, false)
	testParseTags(t, "name-app", map[string]string{}, true)
	testParseTags(t, "name=-a\"pp", map[string]string{}, true)
}

func testRemoveCurlyBraces(t *testing.T, in, out string, errOk bool) {
	out2, err := RemoveCurlyBraces(in)
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

func testsplitString(t *testing.T, in string, exp []string, errOk bool) {
	res, err := SplitString(in, '=', ',', []string{})
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

func testParseTags(t *testing.T, kvs string, expm map[string]string, errOk bool) {
	m, err := ToMap(kvs)
	if err != nil {
		if errOk {
			return
		}
		t.Fatal("unexpected err=", err)
	}

	if errOk {
		t.Fatal("must be an error, but it is nil")
	}

	if !MapsEquals(m, expm) {
		t.Fatal("expected ", expm, " but returned", m)
	}
}
