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

package tindex

import (
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkFindIndex(b *testing.B) {
	dir, err := ioutil.TempDir("", "benchmark")
	if err != nil {
		b.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true}
	ims.Init(nil)
	tags := "a=b|c=asdfasdfasdf"
	src, err := ims.GetOrCreateJournal(tags)
	if err != nil {
		b.Fatal("Must be able to create new journal")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := ims.GetOrCreateJournal(tags)
		if err != nil || s != src {
			b.Fatal("err must be nil, and s=", s, " must be ", src)
		}
	}
}

func TestCheckInit(t *testing.T) {
	dir, err := ioutil.TempDir("", "CheckInit")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: false}
	err = ims.Init(nil)
	if err == nil {
		t.Fatal("it must return error for CreateNew == false")
	}

	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
}

func TestCheckLoadReload(t *testing.T) {
	dir, err := ioutil.TempDir("", "CheckLoadReload")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up
	log4g.SetLogLevel("", log4g.DEBUG)

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("Init() err=", err)
	}
	tags := "dda=basdfasdf|c=asdfasdfasdf"
	src, _ := ims.GetOrCreateJournal(tags)
	src2, _ := ims.GetOrCreateJournal(tags)
	if src != src2 {
		t.Fatal("Src=", src, " must be equal to src2=", src2)
	}

	ims.Shutdown()
	_, err = ims.GetOrCreateJournal(tags)
	if err == nil {
		t.Fatal("it must be an error here!")
	}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("Init() err=", err)
	}
	src2, err = ims.GetOrCreateJournal(tags)
	if err != nil || src != src2 {
		t.Fatal("err must be nil and src2==Src but src2=", src2, ", Src=", src)
	}

	src2, err = ims.GetOrCreateJournal(tags + "D")
	if err != nil || src == src2 {
		t.Fatal("err must be nil and src2!=Src but src2=", src2)
	}

}

func TestGetJournals(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true, DoNotSave: true}
	ims.Init(nil)
	tags := "dda=basdfasdf|c=asdfasdfasdf"
	src, _ := ims.GetOrCreateJournal(tags)
	tags2 := tags + "ddd"
	src2, _ := ims.GetOrCreateJournal(tags2)

	t1, _ := model.NewTags(tags)
	tl1 := t1.GetTagLine()
	t2, _ := model.NewTags(tags2)
	tl2 := t2.GetTagLine()

	exp, err := lql.ParseExpr("dda=basdfasdf")
	if err != nil {
		t.Fatal("err must be nil, but ", err)
	}

	res, _, err := ims.GetJournals(exp, 10, false)
	if err != nil || len(res) != 2 || res[tl1] != src || res[tl2] != src2 {
		t.Fatal("err=", err, " res=", res, ", Src=", src, ", src2=", src2)
	}

	exp, _ = lql.ParseExpr("dda=basdfasdf  and c=asdfasdfasdf")
	res, _, err = ims.GetJournals(exp, 10, false)
	if err != nil || len(res) != 1 || res[tl1] != src {
		t.Fatal("err=", err, " res=", res, ", Src=", src)
	}

	exp, _ = lql.ParseExpr("a=234")
	res, _, err = ims.GetJournals(exp, 5, false)
	if err != nil || len(res) != 0 {
		t.Fatal("err=", err, " res=", res)
	}

}

func TestGetJournalsLimits(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true, DoNotSave: true}
	ims.Init(nil)
	ims.GetOrCreateJournal("name=app1|ip=123")
	ims.GetOrCreateJournal("name=app1|ip=1233")
	ims.GetOrCreateJournal("name=app1|ip=12334")

	exp, err := lql.ParseExpr("name=app1")
	if err != nil {
		t.Fatal("err must be nil, but ", err)
	}

	res, _, err := ims.GetJournals(exp, 1, false)
	if err != nil || len(res) != 1 {
		t.Fatal("err=", err, " res=", res)
	}

	res, cnt, err := ims.GetJournals(exp, 10, true)
	if err != nil || len(res) != 3 || cnt != 3 {
		t.Fatal("err=", err, " res=", res, ", cnt=", cnt)
	}

	res, cnt, err = ims.GetJournals(exp, 2, false)
	if err != nil || len(res) != 2 || cnt != 3 {
		t.Fatal("err=", err, " res=", res, ", cnt=", cnt)
	}

}
