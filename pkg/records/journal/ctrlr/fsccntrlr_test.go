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

package ctrlr

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/jrivets/log4g"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk/chunkfs"
)

type fsccTest struct {
	dir    string
	fdPool *chunkfs.FdPool
}

func TestFsCCScan(t *testing.T) {
	ft := initFT(t)
	defer ft.done()

	fmt.Println("dir=", ft.dir)
	ft.createChunk(t, "BB.dat", records.SrtingsIterator("aaa", "bbb"))
	ft.createChunk(t, "AA.dat", records.SrtingsIterator("aaa", "bbb"))
	fc := ft.newFSChnksController()
	for i := 0; i < 3; i++ {
		cks, err := fc.scan()
		if err != nil || len(cks) != 2 || cks[0] != 0xAA || cks[1] != 0xBB {
			t.Fatal("i=", i, " Unexpected err=", err, cks)
		}
	}

	if len(fc.knwnChunks) != 2 {
		t.Fatal("wrong fc.knwnChunks=", fc.knwnChunks, ", 2 elements are expected there")
	}

	c1 := fc.knwnChunks[0xAA]
	c1.state = fsChunkStateError
	cks, err := fc.scan()
	if err != nil || len(cks) != 1 || cks[0] != 0xBB || len(fc.knwnChunks) != 2 {
		t.Fatal(" Unexpected err=", err, cks, ccChunkStateName(fc.knwnChunks[0xAA].state))
	}

	os.Remove(path.Join(fc.dir, "BB.dat"))
	cks, err = fc.scan()
	if err != nil || len(cks) != 0 || len(fc.knwnChunks) != 1 {
		t.Fatal(" Unexpected err=", err, cks)
	}

	fc.close()
	_, err = fc.scan()
	if err == nil || len(fc.knwnChunks) != 0 {
		t.Fatal("must be an error after close(), fc.knwnChunks=", fc.knwnChunks)
	}
}

func TestFsGetChunks(t *testing.T) {
	log4g.SetLogLevel("", log4g.DEBUG)
	ft := initFT(t)
	defer ft.done()

	ft.createChunk(t, chunkfs.MakeChunkFileName("", 0xBB), records.SrtingsIterator("aaa", "bbb"))
	ft.createChunk(t, chunkfs.MakeChunkFileName("", 0xAA), records.SrtingsIterator("aaa", "bbb"))
	fc := ft.newFSChnksController()
	fc.scan()
	if len(fc.knwnChunks) != 2 || fc.state != fsCCStateScanned {
		t.Fatal("wrong fc.knwnChunks=", fc.knwnChunks, ", 2 elements are expected there")
	}

	cks, err := fc.getChunks(context.Background())
	if err != nil || len(cks) != 2 {
		t.Fatal("err=", err, " len(cks)=", len(cks), " fc.knwnChunks[0xAA]=", fc.knwnChunks[0xAA])
	}

	// phantom
	os.Remove(path.Join(fc.dir, chunkfs.MakeChunkFileName("", 0xBB)))
	cids, err := fc.scan()
	if err != nil || len(cids) != 1 {
		t.Fatal(" Unexpected err=", err, ", or cids=", cids, " len=", len(cids))
	}

	cks, err = fc.getChunks(context.Background())
	if err != nil || len(cks) != 1 {
		t.Fatal("err=", err, " len(cks)=", len(cks), " fc.knwnChunks[0xAA]=", fc.knwnChunks[0xAA])
	}

	// phantom 2
	it, err := cks[0].Iterator()
	os.Remove(path.Join(fc.dir, chunkfs.MakeChunkFileName("", 0xAA)))
	cids, err = fc.scan()
	if err != nil || len(cids) != 0 {
		t.Fatal(" Unexpected err=", err, ", or cids=", cids, " len=", len(cids))
	}

	time.Sleep(time.Millisecond)
	if len(fc.knwnChunks) != 1 || fc.knwnChunks[0xAA].state != fsChunkStateDeleting {
		t.Fatal("Must be there, but ", ccChunkStateName(fc.knwnChunks[0xAA].state))
	}
	it.Close()
	time.Sleep(time.Millisecond)
	if len(fc.knwnChunks) != 0 {
		t.Fatal("Must be no chunks anymmore ")
	}
}

func TestFsGetWriteChunk(t *testing.T) {
	log4g.SetLogLevel("", log4g.DEBUG)
	ft := initFT(t)
	defer ft.done()

	fc := ft.newFSChnksController()
	fc.scan()

	c, nw, err := fc.getChunkForWrite(context.Background())
	if c == nil || !nw || err != nil {
		t.Fatal("expecting c=nil, nw=true, err=nil, but c=", c, ", nw=", nw, ", err=", err)
	}

	c, nw, err = fc.getChunkForWrite(context.Background())
	if c == nil || nw || err != nil {
		t.Fatal("expecting c=nil, nw=false, err=nil, but c=", c, ", nw=", nw, ", err=", err)
	}

	c.Write(context.Background(), records.SrtingsIterator("aaa", "bbb"))
	c.Sync()
	c1, nw, err := fc.getChunkForWrite(context.Background())
	if c1 == nil || !nw || err != nil || c == c1 {
		t.Fatal("expecting c=nil, nw=true, err=nil, but c=", c, ", nw=", nw, ", err=", err)
	}
}

func (ft *fsccTest) createChunk(t *testing.T, cname string, it records.Iterator) {
	cfg := chunkfs.Config{FileName: path.Join(ft.dir, cname), MaxChunkSize: 1024}
	c, err := chunkfs.New(context.Background(), cfg, ft.fdPool)
	if err != nil {
		t.Fatal("Must be able to create file")
	}

	_, _, err = c.Write(context.Background(), it)
	if err != nil {
		t.Fatal("expecting err=nil, but err=", err)
	}

	c.Sync()
	c.Close()
}

func (ft *fsccTest) newFSChnksController() *fsChnksController {
	cfg := chunkfs.Config{MaxChunkSize: 5}
	return newFSChnksController("test", ft.dir, ft.fdPool, cfg)
}

func initFT(t *testing.T) *fsccTest {
	ft := &fsccTest{}
	var err error
	ft.dir, err = ioutil.TempDir("", "fscccntrlr")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	ft.fdPool = chunkfs.NewFdPool(20)
	return ft
}

func (ft *fsccTest) done() {
	ft.fdPool.Close()
	if ft.dir != "" {
		os.RemoveAll(ft.dir)
	}
}
