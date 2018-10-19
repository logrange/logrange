package fs

import (
	"io/ioutil"
	"math"
	"os"
	"path"
	"testing"
)

func TestReaderOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "writerTestReaderOpen")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "123.dat")
	t.Log(fn)
	r := newFReader(fn, 100)
	err = r.open()
	if err == nil {
		t.Fatal("expected not to open the file, err=", err)
	}

	f, _ := openOrCreateFile(fn)
	f.Close()
	r.r = 100
	r.w = 300
	err = r.open()
	if err != nil {
		t.Fatal("expected to be opened, err=", err)
	}
	if r.r != 0 || r.w != 0 {
		t.Fatal("buffer must be reset")
	}

	r.Close()
	if r.fd != nil {
		t.Fatal("expected to be closed")
	}

	err = r.open()
	if err != nil {
		t.Fatal("expected to be opened ok, err=", err)
	}
	r.Close()
}

func TestReaderSeek(t *testing.T) {
	dir, err := ioutil.TempDir("", "writerTestReaderSeek")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "123.dat")
	f, _ := openOrCreateFile(fn)
	var buf [200]byte
	f.Write(buf[:])
	f.Close()

	r := newFReader(fn, 100)
	err = r.open()
	if err != nil {
		t.Fatal("expected to be opened, err=", err)
	}

	err = r.seek(340)
	if err != nil || r.pos != 340 {
		t.Fatal("Should be able to set offset=34")
	}
	n, err := r.Read(buf[:])
	if n > 0 {
		t.Fatal("Strange read n=", n, ", err=", err)
	}

	err = r.seek(34)
	n, err = r.Read(buf[:50])
	if n != 50 || r.pos != 134 || r.r != 50 || r.w != 100 {
		t.Fatal("Strange read ", r, ", err=", err)
	}

	r.seek(34)
	if r.pos != 134 || r.r != 0 || r.w != 100 {
		t.Fatal("Wrong seek 1: ", r)
	}
	r.seek(133)
	if r.pos != 134 || r.r != 99 || r.w != 100 {
		t.Fatal("Wrong seek 2: ", r)
	}
	r.seek(134)
	if r.pos != 134 || r.r != 0 || r.w != 0 {
		t.Fatal("Wrong seek 3: ", r)
	}

	r.seek(4)
	n, err = r.Read(buf[:120])
	if n != 120 || r.pos != 124 || r.r != 0 || r.w != 0 {
		t.Fatal("Strange read 2,n=", n, ", r=", r, ", err=", err)
	}

	r.seek(4)
	n, err = r.Read(buf[:80])
	if n != 80 || r.pos != 104 || r.r != 80 || r.w != 100 {
		t.Fatal("Strange read 3", r, ", err=", err)
	}

	n, err = r.Read(buf[:80])
	if n != 20 || r.pos != 104 || r.r != 100 || r.w != 100 {
		t.Fatal("Strange read 4, n=", n, ", r=", r, ", err=", err)
	}

	r.seek(4)
	n, err = r.read(buf[:80])
	n, err = r.read(buf[:80])
	if n != 80 || r.pos != 200 || r.r != 60 || r.w != 96 {
		t.Fatal("Strange read 5, n=", n, ", r=", r, ", err=", err)
	}
}

func TestSmartSeek(t *testing.T) {
	dir, err := ioutil.TempDir("", "writerTestSmartSeek")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "123.dat")
	f, _ := openOrCreateFile(fn)
	var buf [200]byte
	f.Write(buf[:])
	f.Close()

	r := newFReader(fn, 100)
	err = r.open()
	if err != nil {
		t.Fatal("expected to be opened, err=", err)
	}

	err = r.smartSeek(50, 120)
	if r.pos != 50 || r.r != 0 || r.w != 0 {
		t.Fatal("Wrong smartSeek 1: ", r)
	}
	r.read(buf[:1])
	if r.pos != 150 || r.r != 1 || r.w != 100 {
		t.Fatal("Wrong smartSeek 2: ", r)
	}

	err = r.smartSeek(100, 20)
	if r.pos != 150 || r.r != 50 || r.w != 100 {
		t.Fatal("Wrong smartSeek 3: ", r)
	}

	err = r.smartSeek(140, 20)
	if r.pos != 160 || r.r != 80 || r.w != 100 {
		t.Fatal("Wrong smartSeek 4: ", r)
	}
	err = r.smartSeek(150, 10)
	if r.pos != 160 || r.r != 90 || r.w != 100 {
		t.Fatal("Wrong smartSeek 5: ", r)
	}
	err = r.smartSeek(151, 10)
	if r.pos != 161 || r.r != 90 || r.w != 100 {
		t.Fatal("Wrong smartSeek 6: ", r)
	}

	err = r.smartSeek(92, 10)
	if r.pos != 161 || r.r != 31 || r.w != 100 {
		t.Fatal("Wrong smartSeek 7: ", r)
	}
}

func TestFillBuffer(t *testing.T) {
	dir, err := ioutil.TempDir("", "writerTestFillBuffer")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "123.dat")
	f, _ := openOrCreateFile(fn)
	var buf [200]byte
	f.Write(buf[:])
	f.Close()

	r := newFReader(fn, 100)
	r.open()
	defer r.Close()

	r.fillBuff(-10)
	if r.pos != 100 || r.r != 0 || r.w != 100 {
		t.Fatal("Wrong fillBuffer 1: ", r)
	}

	r.fillBuff(10)
	if r.pos != 110 || r.r != 0 || r.w != 100 {
		t.Fatal("Wrong fillBuffer 2: ", r)
	}

	r.fillBuff(150)
	if r.pos != 200 || r.r != 0 || r.w != 50 {
		t.Fatal("Wrong fillBuffer 3: ", r)
	}
}

func TestDistance(t *testing.T) {
	r := newFReader("11", 100)
	r.pos = 200
	r.w = 100
	if r.distance(100) != 0 || r.distance(150) != 0 || r.distance(199) != 0 {
		t.Fatal("Unexpected distance(199): ", r.distance(199))
	}

	if r.distance(200) != 1 || r.distance(300) != 101 {
		t.Fatal("Unexpected distance(200): ", r.distance(200))
	}
	if r.distance(0) != uint64(200)+math.MaxInt64 {
		t.Fatal("Unexpected distance(0): ", r.distance(0))
	}
}

func openOrCreateFile(file string) (*os.File, error) {
	return os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0640)
}
