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

package rpc

import (
	"bytes"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/logevent"
	bytes2 "github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
	"testing"
)

func BenchmarkIterator(b *testing.B) {
	wp := &writePacket{tags: "aaa=bbb", src: "journal", events: []*api.LogEvent{}}
	for i := 0; i < 1000; i++ {
		wp.events = append(wp.events, &api.LogEvent{2, "measdkjfhalskdfjhalkdfjhalkdfjaksdjflasjdfs2", "kjhasldkfjhaslkfjhasdkfj=hasdklfjhasdlfkjh"})
	}

	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	wp.WriteTo(ow)

	wpi := new(wpIterator)
	wpi.pool = new(bytes2.Pool)
	wpi.tig = logevent.NewTagIdGenerator(20000)
	wpi.init(btb.Bytes())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wpi.Get(nil)
		if err == io.EOF {
			wpi.init(btb.Bytes())
		} else {
			wpi.Next(nil)
		}
	}
}

func TestWritePacket(t *testing.T) {
	wp := &writePacket{tags: "aaa=bbb", src: "journal", events: []*api.LogEvent{
		&api.LogEvent{1, "mes1", ""},
		&api.LogEvent{2, "mes2", "bbb=ttt"},
	}}

	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	wp.WriteTo(ow)
	if len(btb.Bytes()) != wp.WritableSize() {
		t.Fatal("Expected size=", wp.WritableSize(), ", but real size is ", len(btb.Bytes()))
	}

	// now test the iterator
	wpi := new(wpIterator)
	wpi.pool = new(bytes2.Pool)
	wpi.tig = logevent.NewTagIdGenerator(20000)
	wpi.init(btb.Bytes())

	if wpi.src != "journal" || wpi.tags != "aaa=bbb" || wpi.recs != 2 {
		t.Fatal("Wrong wpi=", wpi)
	}

	rec, err := wpi.Get(nil)
	if err != nil {
		t.Fatal("err=", err)
	}
	var le logevent.LogEvent
	le.Unmarshal(rec, false)
	if le.Timestamp != 1 || le.Tags != "aaa=bbb" || le.TgId == 0 || le.Msg != "mes1" {
		t.Fatal("Something wrong with le=", le)
	}

	wpi.Next(nil)
	rec, err = wpi.Get(nil)
	if err != nil {
		t.Fatal("err=", err)
	}
	le.Unmarshal(rec, false)
	if le.Timestamp != 2 || le.Tags != "bbb=ttt" || le.TgId != 0 || le.Msg != "mes2" {
		t.Fatal("Something wrong with le=", le)
	}

	wpi.Next(nil)
	_, err = wpi.Get(nil)
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but err=", err)
	}
}
