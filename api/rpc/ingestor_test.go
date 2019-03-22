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

package rpc

import (
	"bytes"
	"github.com/logrange/logrange/api"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
	"testing"
)

func BenchmarkIterator(b *testing.B) {
	wp := &writePacket{tags: "aaa=bbb", events: []*api.LogEvent{}}
	for i := 0; i < 1000; i++ {
		wp.events = append(wp.events, &api.LogEvent{2, "measdkjfhalskdfjhalkdfjhalkdfjaksdjflasjdfs2", "kjhasldkfjhaslkfjhasdkfj=hasdklfjhasdlfkjh", "a=b"})
	}

	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	wp.WriteTo(ow)

	wpi := new(wpIterator)
	wpi.init(btb.Bytes())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := wpi.Get(nil)
		if err == io.EOF {
			wpi.init(btb.Bytes())
		} else {
			wpi.Next(nil)
		}
	}
}

func TestWritePacket(t *testing.T) {
	wp := &writePacket{tags: "aaa=bbb", events: []*api.LogEvent{
		&api.LogEvent{1, "mes1", "", "a=b"},
		&api.LogEvent{2, "mes2", "bbb=ttt", "a=b"},
	}}

	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	wp.WriteTo(ow)
	if len(btb.Bytes()) != wp.WritableSize() {
		t.Fatal("Expected size=", wp.WritableSize(), ", but real size is ", len(btb.Bytes()))
	}

	src := "aaa=bbb"
	// now test the iterator
	wpi := new(wpIterator)
	wpi.init(btb.Bytes())

	if wpi.tags != src || wpi.recs != 2 {
		t.Fatal("Wrong wpi=", wpi)
	}

	le, _, err := wpi.Get(nil)
	if err != nil {
		t.Fatal("err=", err)
	}
	if le.Timestamp != 1 || le.Msg.AsWeakString() != "mes1" {
		t.Fatal("Something wrong with le=", le)
	}

	wpi.Next(nil)
	le, _, err = wpi.Get(nil)
	if err != nil {
		t.Fatal("err=", err)
	}
	if le.Timestamp != 2 || le.Msg.AsWeakString() != "mes2" {
		t.Fatal("Something wrong with le=", le)
	}

	wpi.Next(nil)
	_, _, err = wpi.Get(nil)
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but err=", err)
	}
}

func TestWritePacketWithTindex(t *testing.T) {
	wp := &writePacket{tags: "aaa=bbb", events: []*api.LogEvent{
		&api.LogEvent{1, "mes1", "", "a=b"},
		&api.LogEvent{2, "mes2", "bbb=ttt", "a=b"},
	}}

	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	wp.WriteTo(ow)
	if len(btb.Bytes()) != wp.WritableSize() {
		t.Fatal("Expected size=", wp.WritableSize(), ", but real size is ", len(btb.Bytes()))
	}

	// now test the iterator
	wpi := new(wpIterator)
	wpi.init(btb.Bytes())

	if wpi.tags != wp.tags || wpi.recs != 2 {
		t.Fatal("Wrong wpi=", wpi)
	}

	le, _, err := wpi.Get(nil)
	if err != nil {
		t.Fatal("err=", err)
	}
	if le.Timestamp != 1 || le.Msg.AsWeakString() != "mes1" {
		t.Fatal("Something wrong with le=", le)
	}

	wpi.Next(nil)
	le, _, err = wpi.Get(nil)
	if err != nil {
		t.Fatal("err=", err)
	}
	if le.Timestamp != 2 || le.Msg.AsWeakString() != "mes2" {
		t.Fatal("Something wrong with le=", le)
	}

	wpi.Next(nil)
	_, _, err = wpi.Get(nil)
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but err=", err)
	}
}
