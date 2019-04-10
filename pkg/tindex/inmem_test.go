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
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/journal"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type testJournals struct {
	js []string
}

func (tjc *testJournals) Visit(ctx context.Context, cv journal.ControllerVisitorF) {
	for _, jn := range tjc.js {
		if !cv(&testJrnl{jn}) {
			return
		}
	}
}

func (tjc *testJournals) GetOrCreate(ctx context.Context, jname string) (journal.Journal, error) {
	return nil, nil
}

func (tjc *testJournals) Delete(ctx context.Context, jname string) error {
	return nil
}

type testJrnl struct {
	name string
}

func (tj *testJrnl) Name() string {
	return tj.name
}

func (tj *testJrnl) Write(ctx context.Context, rit records.Iterator) (int, journal.Pos, error) {
	return 0, journal.Pos{}, nil
}

func (tj *testJrnl) Size() uint64 {
	return 0
}

func (tj *testJrnl) Count() uint64 {
	return 0
}

func (tj *testJrnl) Iterator() journal.Iterator {
	return nil
}

func (tj *testJrnl) Sync() {

}

func (tj *testJrnl) Chunks() journal.ChnksController {
	return nil
}

func BenchmarkFindIndex(b *testing.B) {
	dir, err := ioutil.TempDir("", "benchmark")
	if err != nil {
		b.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Config = &InMemConfig{WorkingDir: dir}
	ims.Journals = &testJournals{}
	ims.Init(nil)
	src, _, err := ims.GetOrCreateJournal("a=b,c=asdfasdfasdf")
	if err != nil {
		b.Fatal("Must be able to create new partition")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _, err := ims.GetOrCreateJournal("a=b,c=asdfasdfasdf")
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
	ims.Journals = &testJournals{}
	ims.Config = &InMemConfig{WorkingDir: dir, DoNotSave: false}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}

	jrnl, ts, err := ims.GetOrCreateJournal("aaa=bbb")
	if err != nil {
		t.Fatal("Must be created, but err=", err)
	}

	if ts.Tag("aaa") != "bbb" || ts.Line() != "aaa=bbb" {
		t.Fatal("Wrong ts= ", ts)
	}

	ims.Shutdown()

	ims.Journals = &testJournals{[]string{jrnl}}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	ims.Shutdown()

	ims.Journals = &testJournals{[]string{jrnl, "1234"}}
	err = ims.Init(nil)
	if err == nil {
		t.Fatal("err is nil, but must not be")
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
	ims.Journals = &testJournals{}
	ims.Config = &InMemConfig{WorkingDir: dir}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("Init() err=", err)
	}
	set, _ := tag.Parse("{dda=basdfasdf,c=asdfasdfasdf}")
	tags := set.Line()
	src, _, _ := ims.GetOrCreateJournal("dda=basdfasdf,c=asdfasdfasdf")
	src2, _, _ := ims.GetOrCreateJournal("c=asdfasdfasdf,dda=basdfasdf")
	if src != src2 {
		t.Fatal("Src=", src, " must be equal to src2=", src2)
	}

	ims.Shutdown()
	_, _, err = ims.GetOrCreateJournal(string(tags))
	if err == nil {
		t.Fatal("it must be an error here!")
	}
	err = ims.Init(nil)
	if err != nil {
		t.Fatal("Init() err=", err)
	}
	src2, _, err = ims.GetOrCreateJournal(string(tags))
	if err != nil || src != src2 {
		t.Fatal("err must be nil and src2==Src but src2=", src2, ", Src=", src)
	}

	src2, _, err = ims.GetOrCreateJournal("{dda=basdfasdf,c=asdfasdfasdfSSS}")
	if err != nil || src == src2 {
		t.Fatal("err must be nil and src2!=Src but src2=", src2)
	}

}

func TestVisitor(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Journals = &testJournals{}
	ims.Config = &InMemConfig{WorkingDir: dir, DoNotSave: true}
	ims.Init(nil)
	set, _ := tag.Parse("{dda=basdfasdf,c=asdfasdfasdf}")
	tags := set.Line()
	src, _, _ := ims.GetOrCreateJournal("dda=basdfasdf,c=asdfasdfasdf")
	set, _ = tag.Parse("{dda=basdfasdf,c=asdfasdfasdfddd}")
	tags2 := set.Line()
	src2, _, _ := ims.GetOrCreateJournal("dda=basdfasdf,c=asdfasdfasdfddd")

	ps, _ := lql.ParseSource("{dda=basdfasdf}")
	res, err := getJournals(ims, ps)
	if err != nil || len(res) != 2 || res[tags] != src || res[tags2] != src2 {
		t.Fatal("err=", err, " res=", res, ", Src=", src, ", src2=", src2)
	}

	ps, _ = lql.ParseSource("dda=basdfasdf  and c=asdfasdfasdf")
	res, err = getJournals(ims, ps)
	if err != nil || len(res) != 1 || res[tags] != src {
		t.Fatal("err=", err, " res=", res, ", Src=", src)
	}

	ps, _ = lql.ParseSource("{c=asdfasdfasdf,dda=basdfasdf}")
	res, err = getJournals(ims, ps)
	if err != nil || len(res) != 1 || res[tags] != src {
		t.Fatal("err=", err, " res=", res, ", Src=", src)
	}

	ps, _ = lql.ParseSource("{" + string(tags) + "}")
	res, err = getJournals(ims, ps)
	if err != nil || len(res) != 1 || res[tags] != src {
		t.Fatal("err=", err, " res=", res, ", Src=", src)
	}

	ps, _ = lql.ParseSource("a=234")
	res, err = getJournals(ims, ps)
	if err != nil || len(res) != 0 {
		t.Fatal("err=", err, " res=", res)
	}
}

func TestReAcquireExclusively(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	tags := tag.Line("dda=a")
	ps, _ := lql.ParseSource(tags.String())

	ims := NewInmemService().(*inmemService)
	ims.Journals = &testJournals{}
	ims.Config = &InMemConfig{WorkingDir: dir, DoNotSave: true}
	ims.Init(nil)

	if ims.LockExclusively("blah") {
		t.Fatal("Must not be acquired")
	}

	src, _, _ := ims.GetOrCreateJournal("dda=a") //1st
	ims.GetOrCreateJournal("dda=a")              // 2nd

	if ims.tmap["dda=a"].readers != 2 {
		t.Fatal("Wrong value for td=", ims.tmap["dda=a"])
	}

	res, _ := getJournals(ims, ps)

	// check visitior
	if len(res) != 1 || res[tags] != src {
		t.Fatal("Visitor must return the partition but res=", res)
	}

	if ims.smap[src].readers != 2 {
		t.Fatal("Wrong value for td=", ims.smap[src])
	}

	ok := ims.LockExclusively(src)
	if ok || ims.smap[src].exclusive {
		t.Fatal("Must not be acquired but ok=", ok)
	}
	ims.Release(src)

	// Now, we have only one read ackusition, so can re-acquire for exclusive
	if !ims.LockExclusively(src) || !ims.smap[src].exclusive {
		t.Fatal("Must be acquired")
	}

	// visitor must not return the partition
	res, _ = getJournals(ims, ps)
	if len(res) != 0 {
		t.Fatal("Visitor must not return the partition but res=", res)
	}

	// releasing the exclusive
	ims.UnlockExclusively(src)
	if ims.smap[src].exclusive {
		t.Fatal("Must not be exclusive")
	}
	ims.Release(src)

	// not acquired for read, so must fail
	if ims.LockExclusively(src) || ims.smap[src].exclusive {
		t.Fatal("Must not be acquired")
	}

	// visitor must return the partition again
	res, _ = getJournals(ims, ps)
	if len(res) != 1 || res[tags] != src {
		t.Fatal("Visitor must return the partition but res=", res)
	}

	// Ok, re-acquire once again now
	ims.GetOrCreateJournal("dda=a")
	if !ims.LockExclusively(src) || !ims.smap[src].exclusive {
		t.Fatal("Must be acquired")
	}

	// will check that GetOrCreateJournal until the exclusive is released...
	var nextAck int64
	go func() {
		ims.GetOrCreateJournal("dda=a")
		atomic.StoreInt64(&nextAck, time.Now().UnixNano())
		ims.Release(src)
	}()

	time.Sleep(10 * time.Millisecond)
	now := time.Now()
	ims.UnlockExclusively(src)
	ims.Release(src)
	time.Sleep(2 * time.Millisecond)
	if atomic.LoadInt64(&nextAck)-now.UnixNano() < 0 {
		t.Fatal("concurrent acquisition in go routine at ", nextAck, " must happen after the release ", now)
	}
}

func TestDelete(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Journals = &testJournals{}
	ims.Config = &InMemConfig{WorkingDir: dir, DoNotSave: true}
	ims.Init(nil)

	src, _, _ := ims.GetOrCreateJournal("dda=a") //1st
	// on not exclusive lock must fail
	if ims.Delete(src) == nil {
		t.Fatal("Must not be able to Delete!")
	}

	ims.Release(src)

	// must fail if no lock at all
	if ims.Delete(src) == nil {
		t.Fatal("Must not be able to Delete!")
	}

	if len(ims.smap) != 1 || len(ims.tmap) != 1 {
		t.Fatal("Must not affect data, but ims.smap=", ims.smap)
	}

	// ok, no get it exclusively
	ims.GetOrCreateJournal("dda=a")
	ims.LockExclusively(src)
	if err := ims.Delete(src); err != nil {
		t.Fatal("Must be able to delete, but err=", err)
	}

	ims.Release(src)
	if len(ims.smap) != 0 || len(ims.tmap) != 0 {
		t.Fatal("Must not affect data, but ims.smap=", ims.smap)
	}
}

func TestVisitSkipping(t *testing.T) {
	dir, err := ioutil.TempDir("", "GetJournals")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ims := NewInmemService().(*inmemService)
	ims.Journals = &testJournals{}
	ims.Config = &InMemConfig{WorkingDir: dir, DoNotSave: true}
	ims.Init(nil)

	src, _, _ := ims.GetOrCreateJournal("dda=a") //1st

	if ims.smap[src].readers != 1 {
		t.Fatal("Must be 1 reader here")
	}

	ims.Release(src)

	// Visit release
	ims.visitSkippingIfLocked(lql.PositiveTagsExpFunc, func(tags tag.Set, jrnl string) bool {
		if ims.smap[jrnl].readers != 1 {
			t.Fatal("Must be 1 reader here")
		}
		return true
	}, 0)

	if ims.smap[src].readers != 0 {
		t.Fatal("Must be 0 readers here")
	}

	// Visit non-release
	ims.visitSkippingIfLocked(lql.PositiveTagsExpFunc, func(tags tag.Set, jrnl string) bool {
		if ims.smap[jrnl].readers != 1 {
			t.Fatal("Must be 1 reader here")
		}
		return true
	}, VF_DO_NOT_RELEASE)

	if ims.smap[src].readers != 1 {
		t.Fatal("Must be 0 readers here")
	}
	ims.Release(src)

	src2, _, _ := ims.GetOrCreateJournal("dda3=a") //1st
	ims.Release(src2)

	// Visit non-release
	visited := map[string]string{}
	ims.visitSkippingIfLocked(lql.PositiveTagsExpFunc, func(tags tag.Set, jrnl string) bool {
		if ims.smap[jrnl].readers != 1 {
			t.Fatal("Must be 1 reader here")
		}
		visited[jrnl] = jrnl
		return false
	}, VF_DO_NOT_RELEASE)

	if len(visited) != 1 || (visited[src] == src && ims.smap[src2].readers != 0) || (visited[src2] == src2 && ims.smap[src].readers != 0) {
		t.Fatal("not properly released ", visited)
	}

	if visited[src] == src {
		ims.Release(src)
	} else {
		ims.Release(src2)
	}

	// Visit release
	visited = map[string]string{}
	ims.visitSkippingIfLocked(lql.PositiveTagsExpFunc, func(tags tag.Set, jrnl string) bool {
		if ims.smap[jrnl].readers != 1 {
			t.Fatal("Must be 1 reader here")
		}
		visited[jrnl] = jrnl
		return false
	}, 0)

	if len(visited) != 1 || ims.smap[src2].readers != 0 || ims.smap[src].readers != 0 {
		t.Fatal("not properly released ", visited)
	}
}

func getJournals(ims *inmemService, srcCond *lql.Source) (map[tag.Line]string, error) {
	res := make(map[tag.Line]string)
	err := ims.Visit(srcCond, func(tags tag.Set, jrnl string) bool {
		res[tags.Line()] = jrnl
		return true
	}, VF_SKIP_IF_LOCKED)
	return res, err
}
