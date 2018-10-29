package fs

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/logrange/logrange/pkg/records/inmem"
)

func TestCReaderReadWhatIsWritten(t *testing.T) {
	dir, err := ioutil.TempDir("", "creaderWhatIsWrittenTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "tst")
	cw := newCWriter(fn, 0, 0, 1000)
	defer cw.Close()

	si := inmem.SrtingsIterator("aa", "b", "c")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	var w inmem.Writer
	buf := make([]byte, 100)
	w.Reset(buf, false)
	fr, _ := newFReader(fn, 1024)
	defer fr.Close()

	cr := cReader{fr, &cw.lro}

	// forward, read all
	n, offs, err := cr.readForward(0, 1000, &w)
	if n != 3 || offs != cw.lro+9 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"aa", "b", "c"})

	//small buf
	w.Reset(make([]byte, 1), false)
	n, offs, err = cr.readForward(0, 1000, &w)
	if n != 0 || err != ErrBufferTooSmall {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()

	// forward read all starting from second record
	w.Reset(buf, false)
	n, offs, err = cr.readForward(10, 1000, &w)
	if n != 2 || offs != cw.lro+9 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"b", "c"})

	// read out of range
	w.Reset(buf, false)
	n, offs, err = cr.readForward(99, 1000, &w)
	if n != 0 || err != io.EOF {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{})

	// read statring from 3rd record
	w.Reset(buf, false)
	n, offs, err = cr.readForward(19, 1000, &w)
	if n != 1 || err != nil || offs != 28 {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"c"})

	// read one starting from the second record
	w.Reset(buf, false)
	n, offs, err = cr.readForward(10, 1, &w)
	if n != 1 || err != nil || offs != 19 {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"b"})

	// read 0 starting from 2nd record
	w.Reset(buf, false)
	n, offs, err = cr.readForward(10, 0, &w)
	if n != 0 || err != nil || offs != 10 {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{})

	// read with wrong offset
	w.Reset(buf, false)
	n, offs, err = cr.readForward(9, 1, &w)
	if n != 0 || (err != ErrCorruptedData && err != ErrBufferTooSmall) || offs != 9 {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
}

func TestCReaderReadWhatIsWrittenBack(t *testing.T) {
	dir, err := ioutil.TempDir("", "creaderWhatIsWrittenTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "tst")
	cw := newCWriter(fn, 0, 0, 1000)
	defer cw.Close()

	si := inmem.SrtingsIterator("aa", "b", "c")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	var w inmem.Writer
	buf := make([]byte, 100)
	w.Reset(buf, false)
	fr, _ := newFReader(fn, 1024)
	defer fr.Close()

	cr := cReader{fr, &cw.lro}

	//small buf
	w.Reset(make([]byte, 1), false)
	n, offs, err := cr.readBack(0, 1000, &w)
	if n != 0 || err != ErrBufferTooSmall {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()

	// backward read all starting from first record
	w.Reset(buf, false)
	n, offs, err = cr.readBack(0, 1000, &w)
	if n != 1 || offs != -1 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"aa"})

	// backward read all starting from 2nd record
	w.Reset(buf, false)
	n, offs, err = cr.readBack(10, 1000, &w)
	if n != 2 || offs != -1 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"b", "aa"})

	// backward read all starting from last record
	w.Reset(buf, false)
	n, offs, err = cr.readBack(100, 1000, &w)
	if n != 3 || offs != -1 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"c", "b", "aa"})

	// backward read 2 starting from last record
	w.Reset(buf, false)
	n, offs, err = cr.readBack(*cr.lro, 2, &w)
	if n != 2 || offs != 0 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"c", "b"})

	// backward read 1 starting from last record
	w.Reset(buf, false)
	n, offs, err = cr.readBack(*cr.lro, 1, &w)
	if n != 1 || offs != 10 || err != nil {
		t.Fatal("n=", n, ", offs=", offs, ", err=", err, ", lro=", *cr.lro)
	}
	w.Close()
	testStrSlices(t, buf, []string{"c"})

}

func testStrSlices(t *testing.T, buf []byte, strs []string) {
	s, err := inmem.ReadBufAsStringSlice(inmem.Records(buf))
	if err != nil {
		t.Fatal("Could not read buffer as a slice of strings err=", err)
	}

	if !testSlicesEquals(s, strs) {
		t.Fatal("Received data s=", s, " is not equal to strs=", strs)
	}
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
