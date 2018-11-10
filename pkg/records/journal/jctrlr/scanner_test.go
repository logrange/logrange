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

package jctrlr

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/records/chunk/chunkfs"
	"github.com/logrange/logrange/pkg/util"
)

func TestScannerScan(t *testing.T) {
	dir, err := ioutil.TempDir("", "TestScannerScan")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	scr := &scanner{log4g.GetLogger("scanner")}
	res, err := scr.scan(dir)
	if err != nil || len(res) > 0 {
		t.Fatal("should be empty result and err=nil, but err=", err, ", res=", res)
	}

	prepareDirForScanTests(dir, scJournal{name: "test"})
	res, err = scr.scan(dir)
	if err != nil || len(res) != 1 {
		t.Fatal("Expecting err=nil, but err=", err, ", res=", res)
	}
	ep, _ := journalPath(dir, "test")
	if res[0].name != "test" || res[0].dir != ep || len(res[0].chunks) > 0 {
		t.Fatal("Wrong res=", res)
	}

	prepareDirForScanTests(dir, scJournal{name: "test", chunks: []chunk.Id{1, 22}})
	res, err = scr.scan(dir)
	if err != nil || len(res) != 1 {
		t.Fatal("Expecting err=nil, but err=", err, ", res=", res)
	}
	if len(res[0].chunks) != 2 {
		t.Fatal("Wrong res=", res)
	}

	prepareDirForScanTests(dir, scJournal{name: "test2", chunks: []chunk.Id{1, 22}})
	res, err = scr.scan(dir)
	if err != nil || len(res) != 2 {
		t.Fatal("Expecting err=nil, but err=", err, ", res=", res)
	}
}

func prepareDirForScanTests(dir string, scj scJournal) {
	pth, _ := journalPath(dir, scj.name)
	ensureDirExists(pth)
	for _, cid := range scj.chunks {
		crateFile(chunkfs.MakeChunkFileName(pth, cid))
		crateFile(util.SetFileExt(chunkfs.MakeChunkFileName(pth, cid+1), "aaa"))
		crateFile(util.SetFileExt(chunkfs.MakeChunkFileName(pth, cid+1), "bbb"))
	}
}

func crateFile(file string) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0640)
	if err == nil {
		f.Close()
	}
}
