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
	"bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"reflect"
	"testing"
)

func BenchmarkLogEventMarshal(b *testing.B) {
	le := &LogEvent{1, []byte("asdfasdfasdf asdfasdf asdf"), "al;sdkf';lasdfl;kasdjflkajsdflkjasdflkjlk"}
	var bb [1000]byte
	buf := bb[:]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		le.Marshal(buf)
	}
}

func BenchmarkLogEventWriteTo(b *testing.B) {
	le := &LogEvent{1, []byte("asdfasdfasdf asdfasdf asdf"), "askdfjhasdkfjhaskdjfhaksdjfhkasdjfhkasdjfh"}
	var buf bytes.Buffer
	ow := &xbinary.ObjectsWriter{Writer: &buf}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		le.WriteTo(ow)
	}
}

func TestMarshalEmpty(t *testing.T) {
	le := LogEvent{}
	if le.WritableSize() != 10 {
		t.Fatal("Must be 1, but the size is ", le.WritableSize())
	}

	var bb [11]byte
	n, err := le.Marshal(bb[:])
	if n != 10 || err != nil {
		t.Fatal("n must be 1, but it is ", n, ", err=", err)
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	testMarshalUnmarshal(t, &LogEvent{}, 10)
	testMarshalUnmarshal(t, &LogEvent{Timestamp: 1234}, 10)
	testMarshalUnmarshal(t, &LogEvent{Msg: []byte("abc")}, 13)
	testMarshalUnmarshal(t, &LogEvent{Fields: "abc"}, 14)
	testMarshalUnmarshal(t, &LogEvent{Msg: []byte("a"), Fields: "abc"}, 15)
}

func testMarshalUnmarshal(t *testing.T, le *LogEvent, sz int) {
	if le.WritableSize() != sz {
		t.Fatal("Expected size is ", sz, ", but Size()=", le.WritableSize())
	}

	var bb [1000]byte
	n, err := le.Marshal(bb[:])
	if n != sz || err != nil {
		t.Fatal("n must be ", sz, ", but it is ", n, ", err=", err)
	}

	le2 := &LogEvent{1, []byte("22"), ""}
	n, err = le2.Unmarshal(bb[:], true)
	if n != sz || !reflect.DeepEqual(le2, le) || err != nil {
		t.Fatal("le2=", le2, " must be same as le=", le, ", expected sz=", sz, ", but n=", n, ", err=", err)
	}
}
