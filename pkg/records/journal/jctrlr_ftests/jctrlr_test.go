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
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk/chunkfs"
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
	nv := append(vals[1:], vals[:2]...)
	for i := 0; i < 10; i++ {
		pos.Idx = 1
		tc.readFromJournal(t, "journal", pos, nv)
	}
}

func TestNewChunkCopied(t *testing.T) {
	tc := prepareTestCtx(t, "NewChunkCopiedTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
	vals := []string{"aaaaa", "bbbbb", "ccccc"}
	pos := tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 3)
	if pos.Idx != 3 {
		t.Fatal("pos idx must be 3, but ", pos, " pos.Idx=", pos.Idx)
	}
	pos2 := tc.writeJournal(t, "journal2", records.SrtingsIterator(vals...), 3)
	if pos2.Idx != 3 || pos == pos2 {
		t.Fatal("pos2 idx must be 3, but ", " pos2.Idx=", pos.Idx, " pos=", pos, ", pos2=", pos2)
	}

	jrnl, err := tc.ctrlr.GetOrCreate(context.Background(), "journal")
	if err != nil || jrnl == nil {
		t.Fatal("could not obtain journal err=", err)
	}

	it := jrnl.Iterator()
	it.SetPos(pos)
	_, err = it.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Must read nothing yet, but err=", err)
	}

	// now copy data from journal2 to journal
	cFile := chunkfs.MakeChunkFileName(path.Join(tc.cfg.BaseDir, "l2", "journal2"), pos2.CId)

	err = copyFile(cFile, chunkfs.MakeChunkFileName(path.Join(tc.cfg.BaseDir, "al", "journal"), pos2.CId))
	if err != nil {
		t.Fatal("could not copy files err=", err)
	}

	err = tc.ctrlr.ScanJournal("journal")
	if err != nil {
		t.Fatal("could not scan err=", err)
	}

	// let it be updated
	time.Sleep(10 * time.Millisecond)

	it.Get(context.Background())

	readFromIt(t, it, vals)
	it.Close()

}

func TestReadEmpty(t *testing.T) {
	tc := prepareTestCtx(t, "EmptyReadTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
	jrnl, err := tc.ctrlr.GetOrCreate(context.Background(), "journal")
	if err != nil || jrnl == nil {
		t.Fatal("could not obtain journal err=", err)
	}

	it := jrnl.Iterator()
	_, err = it.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Must read nothing yet, but err=", err)
	}

	vals := []string{"aaaaa"}
	tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 1)
	readFromIt(t, it, vals)
	_, err = it.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Waiting for EOF, but err=", err)
	}
	vals2 := []string{"abbbbbbbbb", "cccccccc"}
	tc.writeJournal(t, "journal", records.SrtingsIterator(vals2...), 2)
	readFromIt(t, it, vals2)
	_, err = it.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Waiting for EOF, but err=", err)
	}

	tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 1)
	readFromIt(t, it, vals)

	it.Close()

}

func TestDropRemovedJournals(t *testing.T) {
	tc := prepareTestCtx(t, "DropRemovedTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
	vals := []string{"aaaaa", "bbbbb", "ccccc"}
	tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 3)
	tc.writeJournal(t, "j2", records.SrtingsIterator(vals...), 3)

	os.RemoveAll(path.Join(tc.cfg.BaseDir, "al", "journal"))
	tc.ctrlr.Scan(false)
	tc.readFromJournal(t, "journal", journal.Pos{}, nil)
	tc.readFromJournal(t, "j2", journal.Pos{}, vals)
}

func TestReopenJournal(t *testing.T) {
	tc := prepareTestCtx(t, "ReopenJournalTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
	vals := []string{"aaaaa", "bbbbb", "ccccc"}
	pos := tc.writeJournal(t, "journal", records.SrtingsIterator(vals...), 3)
	tc.close()
	tc.open()
	tc.ctrlr.Scan(false)
	pos.Idx = 0
	tc.readFromJournal(t, "journal", pos, vals)
	tc.readFromJournal(t, "journal", journal.Pos{}, vals)
}

func (tc *testCtx) readFromJournal(t *testing.T, jname string, pos journal.Pos, recs []string) {
	jrnl, err := tc.ctrlr.GetOrCreate(context.Background(), jname)
	if err != nil || jrnl == nil {
		t.Fatal("could not obtain journal err=", err)
	}

	it := jrnl.Iterator()
	it.SetPos(pos)
	readFromIt(t, it, recs)
	it.Close()
}

func readFromIt(t *testing.T, it journal.Iterator, recs []string) {
	if len(recs) == 0 {
		_, err := it.Get(context.Background())
		if err != io.EOF {
			t.Fatal("expecting io.EOF, but err=", err)
		}
	}
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
}

func (tc *testCtx) writeJournal(t *testing.T, jname string, it records.Iterator, num int) journal.Pos {
	jrnl, err := tc.ctrlr.GetOrCreate(context.Background(), jname)
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
	log4g.SetLogLevel("", log4g.DEBUG)
	t.Log(dir)
	tc := new(testCtx)
	tc.cfg.BaseDir = dir
	tc.cfg.DefChunkConfig.MaxChunkSize = 20
	tc.cfg.DefChunkConfig.WriteFlushMs = 1
	tc.cfg.FdPoolSize = 4 // 2 simultaneously readers only
	tc.open()
	return tc
}

func copyFile(src, dst string) error {
	input, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(dst, input, 0644)
	if err != nil {
		return err
	}
	return nil
}
