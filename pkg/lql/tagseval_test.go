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

package lql

import (
	"github.com/logrange/logrange/pkg/model"
	"testing"
)

func BenchmarkTagsExpGeneral(b *testing.B) {
	e, _ := ParseExpr("name=app1 and ip like 1*")
	fn, _ := BuildTagsExpFunc(e)
	tags := model.TagMap{"name": "app1", "ip": "1.2.3.4"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn(tags)
	}
}

func getTagsExpFunc(t *testing.T, exp string) TagsExpFunc {
	e, err := ParseExpr(exp)
	if err != nil {
		t.Fatal("The expression '", exp, "' must be compiled, but err=", err)
	}

	res, err := BuildTagsExpFunc(e)
	if err != nil {
		t.Fatal("the expression '", exp, "' must be evaluated no problem, but err=", err)
	}

	return res
}

func testTagsExpGeneral(t *testing.T, exp string, tags model.TagMap, expRes bool) {
	tef := getTagsExpFunc(t, exp)
	if tef(tags) != expRes {
		t.Fatal("Expected ", expRes, " for '", exp, "' expression, but got ", !expRes)
	}
}

func TestTagsExpGeneral(t *testing.T) {
	tags := model.TagMap{"name": "app1", "ip": "1.2.3.4"}
	testTagsExpGeneral(t, "a=b", tags, false)
	testTagsExpGeneral(t, "name='app1'", tags, true)
	testTagsExpGeneral(t, "name=app1 and ip like 1*", tags, true)
	testTagsExpGeneral(t, "name=app13 or ip=\"1.2.3.4\"", tags, true)
	testTagsExpGeneral(t, "name=app13 or name=app1", tags, true)
	testTagsExpGeneral(t, "c=''", tags, true)
}
