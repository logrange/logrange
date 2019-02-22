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

package tindex

import (
	"github.com/logrange/logrange/pkg/lql"
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

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true}
	ims.Init(nil)
	tags := "dda=basdfasdf|c=asdfasdfasdf"
	src, _ := ims.GetOrCreateJournal(tags)
	src2, _ := ims.GetOrCreateJournal(tags)
	if src != src2 {
		t.Fatal("src=", src, " must be equal to src2=", src2)
	}

	ims.Shutdown()
	_, err = ims.GetOrCreateJournal(tags)
	if err == nil {
		t.Fatal("it must be an error here!")
	}
	ims.Init(nil)
	src2, err = ims.GetOrCreateJournal(tags)
	if err != nil || src != src2 {
		t.Fatal("err must be nil and src2==src but src2=", src2, ", src=", src)
	}

	src2, err = ims.GetOrCreateJournal(tags + "D")
	if err != nil || src == src2 {
		t.Fatal("err must be nil and src2!=src but src2=", src2)
	}

}

func TestGetJournals(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir, CreateNew: true}
	ims.Init(nil)
	tags := "dda=basdfasdf|c=asdfasdfasdf"
	src, _ := ims.GetOrCreateJournal(tags)
	src2, _ := ims.GetOrCreateJournal(tags + "ddd")

	exp, err := lql.ParseExpr("dda=basdfasdf")
	if err != nil {
		t.Fatal("err must be nil, but ", err)
	}

	res, err := ims.GetJournals(exp)
	if err != nil || len(res) != 2 || !contains(res, src) || !contains(res, src2) {
		t.Fatal("err=", err, " res=", res, ", src=", src, ", src2=", src2)
	}

	exp, _ = lql.ParseExpr("dda=basdfasdf  and c=asdfasdfasdf")
	res, err = ims.GetJournals(exp)
	if err != nil || len(res) != 1 || !contains(res, src) {
		t.Fatal("err=", err, " res=", res, ", src=", src)
	}

	exp, _ = lql.ParseExpr("a=234")
	res, err = ims.GetJournals(exp)
	if err != nil || len(res) != 0 {
		t.Fatal("err=", err, " res=", res)
	}

}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
