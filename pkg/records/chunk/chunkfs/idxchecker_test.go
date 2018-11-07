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

package chunkfs

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
)

func TestIdxCheckerLightCheckOk(t *testing.T) {
	dir, err := ioutil.TempDir("", "idxcheckerLightCheck")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	ic := prepareIdxChecker(t, dir)
	err = ic.LightCheck()
	if err != nil {
		t.Fatal("Must err=nil, but err=", err)
	}

	// corrupt data file
	ln := truncateFile(t, ic.dr.filename, 0)
	changeFileData(t, ic.dr.filename, ln-3, 22)
	ic.dr.resetBuf()
	err = ic.LightCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	truncateFile(t, ic.dr.filename, 2)
	ic.dr.resetBuf()
	err = ic.LightCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	truncateFile(t, ic.dr.filename, 4)
	ic.dr.resetBuf()
	err = ic.LightCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	truncateFile(t, ic.ir.filename, 3)
	ic.ir.resetBuf()
	err = ic.LightCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	ln = truncateFile(t, ic.ir.filename, 5)
	ic.ir.resetBuf()
	err = ic.LightCheck()
	if err != nil {
		t.Fatal("Must be ok, but err=", err)
	}

	changeFileData(t, ic.ir.filename, ln-1, 0xFF)
	ic.ir.resetBuf()
	err = ic.LightCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}
}

func TestIdxCheckerFullCheckOk(t *testing.T) {
	dir, err := ioutil.TempDir("", "idxcheckerFullCheck")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	// check is ok
	ic := prepareIdxChecker(t, dir)
	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must err=nil, but err=", err)
	}

	// corrupt data offset
	b := changeFileData(t, ic.dr.filename, 7, 0xFF)
	ic.dr.resetBuf()
	err = ic.FullCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	changeFileData(t, ic.dr.filename, 7, b)
	ic.dr.resetBuf()
	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must be ok, but err=", err)
	}

	// corrupt idx data
	b = changeFileData(t, ic.ir.filename, 12, 0xFF)
	ic.ir.resetBuf()
	err = ic.FullCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	changeFileData(t, ic.ir.filename, 12, b)
	ic.ir.resetBuf()
	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must be ok, but err=", err)
	}

	// truncate data file
	truncateFile(t, ic.dr.filename, 2)
	ic.dr.resetBuf()
	err = ic.FullCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	truncateFile(t, ic.dr.filename, 4)
	ic.dr.resetBuf()
	err = ic.FullCheck()
	if err == nil {
		t.Fatal("Must be error, but it is nil")
	}

	// truncate idx file
	truncateFile(t, ic.ir.filename, 8)
	ic.ir.resetBuf()
	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must be ok, but err=", err)
	}
}

func TestIdxCheckerRecover(t *testing.T) {
	dir, err := ioutil.TempDir("", "idxcheckerFullCheck")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	// Straigh forward
	ic := prepareIdxChecker(t, dir)
	err = ic.Recover(false)
	if err != nil {
		t.Fatal("Recover must be ok err=", err)
	}

	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must err=nil, but err=", err)
	}

	// corrupt index
	changeFileData(t, ic.ir.filename, 12, 0xFF)
	err = ic.FullCheck()
	if err == nil {
		t.Fatal("Must not be nil")
	}
	err = ic.Recover(false)
	if err != nil {
		t.Fatal("Recover must be ok! but err=", err)
	}
	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must be nil, but err=", err)
	}

	// corrupt last record
	truncateFile(t, ic.dr.filename, 1)
	err = ic.Recover(false)
	if err == nil {
		t.Fatal("Recover must be not ok, truncate is needed", err)
	}

	err = ic.FullCheck()
	if err == nil {
		t.Fatal("Must not be nil")
	}

	err = ic.Recover(true)
	if err != nil {
		t.Fatal("Recover must be ok, should truncate", err)
	}

	err = ic.FullCheck()
	if err != nil {
		t.Fatal("Must be nil, but err=", err)
	}
}

func prepareIdxChecker(t *testing.T, dir string) *IdxChecker {
	fn := path.Join(dir, "tst")
	cw := newCWriter(fn, 0, 1000, 0)
	defer cw.Close()

	si := records.SrtingsIterator("aa", "bb", "cc")
	n, cnt, err := cw.write(nil, si)
	if n != 3 || cnt != 3 || err != nil {
		t.Fatal("Expecting n=3, сnt=3, err=nil, but n=", n, ", offs=", cnt, ", err=", err)
	}

	ic := &IdxChecker{}
	ic.dr, err = newFReader(fn, ChnkReaderBufSize)
	if err != nil {
		t.Fatal("Unexpected err=", err)
	}

	ic.ir, err = newFReader(SetChunkIdxFileExt(fn), ChnkReaderBufSize)
	if err != nil {
		t.Fatal("Unexpected err=", err)
	}
	ic.logger = log4g.GetLogger("test")
	return ic
}

func truncateFile(t *testing.T, fn string, diff int64) int64 {
	fi, err := os.Stat(fn)
	if err != nil {
		t.Fatal("could not get file stat err=", err)
	}

	ns := fi.Size() - diff
	if diff > 0 {
		err := os.Truncate(fn, ns)
		if err != nil {
			t.Fatal("could not truncate file err=", err)
		}
	}
	return ns
}

func changeFileData(t *testing.T, fn string, off int64, val byte) byte {
	f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal("Could not open file for write err=", err)
	}

	b := make([]byte, 1)
	_, err = f.ReadAt(b, off)
	if err != nil {
		t.Fatal("Could not read byte at ", off, " err=", err)
	}

	res := b[0]
	b[0] = val
	_, err = f.WriteAt(b, off)
	if err != nil {
		t.Fatal("Could not write byte at ", off, " err=", err)
	}

	return res
}
