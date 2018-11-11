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

package jctrl_ftests

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/journal"
	"github.com/logrange/logrange/pkg/records/journal/jctrlr"
)

type testCtx struct {
	cfg   jctrlr.Config
	ctrlr *jctrlr.Controller
}

func TestWriteAndRead(t *testing.T) {
	tc := prepareTestCtx(t, "WriteAndReadTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
	vals := []string{"a", "b", "c"}
	pos := tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 3)
	if pos.Idx != 3 {
		t.Fatal("pos idx must be 3, but ", pos, " pos.Idx=", pos.Idx)
	}

	// read 10 times to be sure iterator releases resources at least
	for i := 0; i < 10; i++ {
		pos.Idx = 0
		tc.readFromJournal(t, "journal", pos, vals)
		pos.Idx = 1
		tc.readFromJournal(t, "journal", pos, vals[1:])
	}
}

func TestWriteManyChunksAndRead(t *testing.T) {
	tc := prepareTestCtx(t, "WriteManyChunksAndReadTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
	vals := []string{"aaaaa", "bbbbb", "ccccc"}
	pos := tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 3)
	if pos.Idx != 3 {
		t.Fatal("pos idx must be 3, but ", pos, " pos.Idx=", pos.Idx)
	}
	pos2 := tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 3)
	if pos2.Idx != 3 || pos == pos2 {
		t.Fatal("pos2 idx must be 3, but ", " pos2.Idx=", pos.Idx, " pos=", pos, ", pos2=", pos2)
	}

	// read 10 times to be sure iterator releases resources at least
	for i := 0; i < 10; i++ {
		pos.Idx = 0
		tc.readFromJournal(t, "journal", pos, vals)
		pos2.Idx = 1
		tc.readFromJournal(t, "journal", pos2, vals[1:])
	}
}

func (tc *testCtx) readFromJournal(t *testing.T, jname string, pos journal.Pos, recs []string) {
	jrnl, err := tc.ctrlr.GetOrCreate(context.Background(), "jname")
	if err != nil || jrnl == nil {
		t.Fatal("could not obtain journal err=", err)
	}

	it := jrnl.Iterator()
	it.SetPos(pos)
	for i := 0; i < len(recs); i++ {
		r, err := it.Get(context.Background())
		if err != nil {
			t.Fatal("Uexpected err=", err, ", i=", i)
		}

		if recs[i] != string(r) {
			t.Fatal("Wrong read, expected ", recs[i], " but got ", string(r))
		}
		it.Next(context.Background())
	}
	it.Close()
}

func (tc *testCtx) writeJournal(t *testing.T, jname string, it records.Iterator, num int) journal.Pos {
	jrnl, err := tc.ctrlr.GetOrCreate(context.Background(), "jname")
	if err != nil || jrnl == nil {
		t.Fatal("could not obtain journal err=", err)
	}

	n, pos, err := jrnl.Write(context.Background(), it)
	if n != num || err != nil {
		t.Fatal("expecting n=", num, ", err=nil, but n=", n, " pos=", pos, ", err=", err)
	}

	jrnl.Sync()

	return pos
}

func (tc *testCtx) Shutdown() {
	defer os.RemoveAll(tc.cfg.BaseDir)
	tc.close()
}

func (tc *testCtx) close() {
	if tc.ctrlr != nil {
		tc.ctrlr.Close()
	}
	tc.ctrlr = nil
}

func (tc *testCtx) open() {
	tc.ctrlr = jctrlr.New(tc.cfg)
}

func prepareTestCtx(t *testing.T, dirname string) *testCtx {
	dir, err := ioutil.TempDir("", dirname)
	if err != nil {
		t.Fatal("Could not create new temporary dir ", dirname, " err=", err)
	}
	log4g.SetLogLevel("root", log4g.DEBUG)
	t.Log(dir)
	tc := new(testCtx)
	tc.cfg.BaseDir = dir
	tc.cfg.DefChunkConfig.MaxChunkSize = 20
	tc.cfg.DefChunkConfig.WriteFlushMs = 1
	tc.cfg.FdPoolSize = 4 // 2 simultaneously readers only
	tc.open()
	return tc
}
