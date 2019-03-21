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
	"github.com/logrange/range/pkg/utils/bytes"
	"strconv"
	"testing"
)

func BenchmarkLogEventParser(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := strconv.Unquote(`"aldkfjhal;kdsdf=sfj,lkasdjflasdjflj"`)
		if err != nil {
			b.Fatal(err)
		}
	}
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

func TestParse(t *testing.T) {
	b := []byte{'a', '=', 'b'}
	st, _ := Parse(bytes.ByteArrayToString(b))
	b[2] = 'c'
	if st.Tag("a") != "b" {
		t.Fatal("something wrong with parse! ", st.Tag("a"), st.Line().String())
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
	tm, err := Parse(tags)
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if tm.Line() != line {
		t.Fatal("Expected ", line, " but received ", tm.Line())
	}
}
