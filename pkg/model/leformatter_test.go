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

package model

import (
	"fmt"
	"github.com/logrange/logrange/pkg/model/tag"
	"testing"
	"time"
)

func BenchmarkLogEventParser(b *testing.B) {
	le := LogEvent{Timestamp: uint64(time.Now().UnixNano()), Msg: " Test message"}
	tgs, _ := tag.Parse("aaa=bbb, ccc=ddd")

	fmtParser := FormatParser{fields: []formatField{{frmtFldConst, "SOME CONSTANT "}, {frmtFldTs, time.RFC822},
		{frmtFldMsg, ""}, {frmtFldConst, " Tag aa="}, {frmtFldTag, "aaa"}, {frmtFldTag, "ccc"}, {frmtFldTag, "aaa1234"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmtParser.FormatStr(&le, tgs.Line().String())
	}
}

func TestLogEventFormatter(t *testing.T) {
	le := LogEvent{Timestamp: uint64(time.Now().UnixNano()), Msg: "Test message"}
	tgs, _ := tag.Parse("aaa=bbb,ccc=ddd")

	fmtParser, err := NewFormatParser("{msg}{tags}, aaa={tag:aaa}")
	if err != nil {
		t.Fatal("unexpected parsing err=", err)
	}

	exp := le.Msg + tgs.Line().String() + ", aaa=bbb"
	if fmtParser.FormatStr(&le, tgs.Line().String()) != exp {
		t.Fatal("Expected ", exp, " but really got ", fmtParser.FormatStr(&le, tgs.Line().String()))
	}
}

func TestLogEventFormatter2(t *testing.T) {
	tm := time.Now()
	le := &LogEvent{Timestamp: uint64(tm.UnixNano()), Msg: "test message"}
	tgs, _ := tag.Parse("a=bbb,b=ddd")

	testLogEventFormatter2(t, le, tgs.Line().String(), "AAA{msg}|{tags} aaa={tag:a} {ts}", fmt.Sprintf("AAA%s|%s aaa=%s %s", le.Msg, tgs.Line(), tgs.Tag("a"), time.Unix(0, int64(le.Timestamp)).Format(time.RFC3339)))
	testLogEventFormatter2(t, le, tgs.Line().String(), "AAA", "AAA")
	testLogEventFormatter2(t, le, tgs.Line().String(), "{Msg}", le.Msg)
	testLogEventFormatter2(t, le, tgs.Line().String(), "{Msg} {TAGS}", le.Msg+" "+tgs.Line().String())
	testLogEventFormatter2(t, le, tgs.Line().String(), "{TS:3:04PM}", tm.Format(time.Kitchen))
}

func TestLogEventFormatterErr(t *testing.T) {
	testLogEventFormatterErr(t, "{asdf}")
	testLogEventFormatterErr(t, "{Message}")
	testLogEventFormatterErr(t, "{Msg:}")
	testLogEventFormatterErr(t, "{Messa")
	testLogEventFormatterErr(t, "{tags:}")
	testLogEventFormatterErr(t, "{tag:___")
}

func testLogEventFormatterErr(t *testing.T, exp string) {
	_, err := NewFormatParser(exp)
	if err == nil {
		t.Fatal("must be error, but the following string parsed no problem ", exp)
	}
}

func testLogEventFormatter2(t *testing.T, le *LogEvent, tl string, fmtStr, exp string) {
	fmtParser, err := NewFormatParser(fmtStr)
	if err != nil {
		t.Fatal("uexpected err=", err, " while parsing ", fmtStr)
	}

	if fmtParser.FormatStr(le, tl) != exp {
		t.Fatal("Expected ", exp, " but really got ", fmtParser.FormatStr(le, tl))
	}
}
