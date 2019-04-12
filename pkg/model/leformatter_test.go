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
	"encoding/json"
	"fmt"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/model/tag"
	"testing"
	"time"
)

func BenchmarkLogEventParser(b *testing.B) {
	le := LogEvent{Timestamp: uint64(time.Now().UnixNano()), Msg: []byte(" Test message")}
	tgs, _ := tag.Parse("aaa=bbb, ccc=ddd")

	fmtParser := FormatParser{fields: []formatField{{frmtFldConst, "SOME CONSTANT "}, {frmtFldTs, time.RFC822},
		{frmtFldMsg, ""}, {frmtFldConst, " Tag aa="}, {frmtFldVar, "aaa"}, {frmtFldVar, "ccc"}, {frmtFldVar, "aaa1234"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmtParser.FormatStr(&le, tgs.Line().String())
	}
}

func TestLogEventFormatter(t *testing.T) {
	le := LogEvent{Timestamp: uint64(time.Now().UnixNano()), Msg: []byte("Test message")}
	tgs, _ := tag.Parse("aaa=bbb,ccc=ddd")

	fmtParser, err := NewFormatParser("{msg}{vars}, aaa={vars:aaa}")
	if err != nil {
		t.Fatal("unexpected parsing err=", err)
	}

	exp := le.Msg.AsWeakString() + tgs.Line().String() + ", aaa=bbb"
	if fmtParser.FormatStr(&le, tgs.Line().String()) != exp {
		t.Fatal("Expected ", exp, " but really got ", fmtParser.FormatStr(&le, tgs.Line().String()))
	}
}

func TestLogEventFormatter2(t *testing.T) {
	tm := time.Now()
	le := &LogEvent{Timestamp: uint64(tm.UnixNano()), Msg: []byte("\test\" message"), Fields: field.Parse("field1=value1, field2=value2")}
	tgs, _ := tag.Parse("a=bbb,b=ddd")

	testLogEventFormatter2(t, le, tgs.Line().String(), "AAA{msg}|{vars} aaa={vars:a} {ts}", fmt.Sprintf("AAA%s|%s,%s aaa=%s %s", le.Msg, tgs.Line(), le.Fields.AsKVString(), tgs.Tag("a"), time.Unix(0, int64(le.Timestamp)).Format(time.RFC3339)))
	testLogEventFormatter2(t, le, tgs.Line().String(), "AAA", "AAA")
	testLogEventFormatter2(t, le, tgs.Line().String(), "{Msg}", le.Msg.AsWeakString())
	bb, _ := json.Marshal(le.Msg.AsWeakString())
	testLogEventFormatter2(t, le, tgs.Line().String(), "{Msg.Json()}", string(bb))
	testLogEventFormatter2(t, le, tgs.Line().String(), "{Msg} {VaRS}", le.Msg.AsWeakString()+" "+tgs.Line().String()+","+le.Fields.AsKVString())
	testLogEventFormatter2(t, le, tgs.Line().String(), "{TS.format(3:04PM)}", tm.Format(time.Kitchen))
	testLogEventFormatter2(t, le, tgs.Line().String(), "{tS.Format()}", "")
	testLogEventFormatter2(t, le, tgs.Line().String(), "{vars:field1}", le.Fields.Value("field1"))
	testLogEventFormatter2(t, le, tgs.Line().String(), "{vars:field2}", le.Fields.Value("field2"))
	testLogEventFormatter2(t, le, tgs.Line().String(), "{vars:field3}", "")
}

func TestLogEventFormatterErr(t *testing.T) {
	testLogEventFormatterErr(t, "{asdf}")
	testLogEventFormatterErr(t, "{Message}")
	testLogEventFormatterErr(t, "{Msg.}")
	testLogEventFormatterErr(t, "{Msg.json)}")
	testLogEventFormatterErr(t, "{msg.}")
	testLogEventFormatterErr(t, "{ts.}")
	testLogEventFormatterErr(t, "{ts.format(}")
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
