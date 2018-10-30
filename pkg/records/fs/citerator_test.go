package fs

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/logrange/logrange/pkg/records/inmem"
)

func TestCIteratorCommon(t *testing.T) {
	// prepare the staff
	dir, err := ioutil.TempDir("", "citeratorCommonTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	p := NewFdPool(2)
	defer p.Close()

	fn := path.Join(dir, "tst")
	cw := newCWriter(fn, -1, 0, 1000)
	cw.ensureFWriter() // to create the file
	defer cw.Close()

	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(fn, p, &cw.lroCfrmd, buf)

	// empty it
	_, err = ci.Get()
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}
	ci.Next()

	si := inmem.SrtingsIterator("aa")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err := ci.Get()
	if inmem.ByteArrayToString(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}

	res, err = ci.Get()
	if inmem.ByteArrayToString(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}

	ci.Next()
	_, err = ci.Get()
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	// wrong offset
	ci.SetPos(2)
	_, err = ci.Get()
	if err != ErrCorruptedData && err != ErrBufferTooSmall {
		t.Fatal("Expecting ErrCorruptedData, but got err=", err)
	}

	si = inmem.SrtingsIterator("bb")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	ci.SetPos(0)
	_, err = ci.Get()
	if err != nil {
		t.Fatal("Expecting nil but got err=", err)
	}

	ci.Next()
	res, err = ci.Get()
	if inmem.ByteArrayToString(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}

	ci.Next()
	_, err = ci.Get()
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	// wrong offset
	ci.SetPos(12345)
	_, err = ci.Get()
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	ci.SetPos(0)
	ci.Next()
	res, err = ci.Get()
	if inmem.ByteArrayToString(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}
}
