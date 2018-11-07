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
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/logrange/logrange/pkg/records"
)

func TestCReaderReadByRecords(t *testing.T) {
	dir, err := ioutil.TempDir("", "creaderReadByRecordsTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "tst")
	cw := newCWriter(fn, 0, 1000, 0)
	defer cw.Close()

	si := records.SrtingsIterator("aa", "b")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	dr, _ := newFReader(fn, 1024)
	defer dr.Close()

	ir, _ := newFReader(SetChunkIdxFileExt(fn), 1024)
	defer ir.Close()

	cr := cReader{dr: dr, ir: ir}

	buf := make([]byte, 2)
	_, err = cr.readRecord(buf[:1])
	if err != ErrBufferTooSmall {
		t.Fatal("Expecting ErrBufferTooSmall, but err=", err)
	}

	err = cr.setPos(10)
	if err != nil {
		t.Fatal("Expecting no error, but err=", err)
	}

	cr.setPos(0)
	res, err := cr.readRecord(buf)
	if err != nil || records.ByteArrayToString(res) != "aa" {
		t.Fatal("Expecting nil, but err=", err, " res=", records.ByteArrayToString(res))
	}

	res, err = cr.readRecord(buf)
	if err != nil || records.ByteArrayToString(res) != "b" {
		t.Fatal("Expecting nil, but err=", err, " res=", records.ByteArrayToString(res))
	}

	cr.setPos(1)
	res, err = cr.readRecord(buf)
	if err != nil || string(res) != "b" {
		t.Fatal("Expecting b, but err=", err, " res=", string(res))
	}

	_, err = cr.readRecord(buf)
	_, err = cr.readRecord(buf)
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but err=", err)
	}

	// now read after write...
	si = records.SrtingsIterator("cc")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err = cr.readRecord(buf)
	if err != nil || string(res) != "cc" {
		t.Fatal("Expecting cc, but err=", err, " res=", string(res))
	}

	// setPos far away and then read
	cr.setPos(10)
	si = records.SrtingsIterator("c1")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err = cr.readRecord(buf)
	if err != nil || string(res) != "c1" {
		t.Fatal("Expecting cc, but err=", err, " res=", string(res))
	}

	// Now read all from 2
	cr.setPos(2)
	str := []string{}
	for {
		res, err = cr.readRecord(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal("Unexpected err=", err)
		}
		str = append(str, string(res))
	}
	testSlicesEquals(str, []string{"cc", "c1"})
}

func testSlicesEquals(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
