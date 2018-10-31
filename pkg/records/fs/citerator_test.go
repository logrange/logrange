package fs

import (
	"context"
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

	p.register(123, fn)
	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(123, p, &cw.lroCfrmd, &cw.sizeCfrmd, buf)

	// empty it
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}
	ci.Next(context.Background())

	si := inmem.SrtingsIterator("aa")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err := ci.Get(context.Background())
	if inmem.ByteArrayToString(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}

	res, err = ci.Get(context.Background())
	if inmem.ByteArrayToString(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}

	ci.Next(context.Background())
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	// wrong offset
	ci.SetPos(2)
	_, err = ci.Get(context.Background())
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
	_, err = ci.Get(context.Background())
	if err != nil {
		t.Fatal("Expecting nil but got err=", err)
	}

	ci.Next(context.Background())
	res, err = ci.Get(context.Background())
	if inmem.ByteArrayToString(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}

	ci.Next(context.Background())
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	// wrong offset
	ci.SetPos(12345)
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	ci.SetPos(0)
	ci.Next(context.Background())
	res, err = ci.Get(context.Background())
	if inmem.ByteArrayToString(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", inmem.ByteArrayToString(res), " and err=", err)
	}
}

func TestCIteratorPos(t *testing.T) {
	// prepare the staff
	dir, err := ioutil.TempDir("", "citeratorPosTest")
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

	p.register(123, fn)
	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(123, p, &cw.lroCfrmd, &cw.sizeCfrmd, buf)

	si := inmem.SrtingsIterator("aa")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err := ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	ci.SetPos(-10)
	res, err = ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	// wrong offset
	ci.SetPos(5)
	_, err = ci.Get(context.Background())
	if err != ErrCorruptedData && err != ErrBufferTooSmall {
		t.Fatal("Expecting ErrCorruptedData, but got err=", err)
	}

	ci.SetPos(1235)
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	si = inmem.SrtingsIterator("bb")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err = ci.Get(context.Background())
	if string(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", string(res), " and err=", err)
	}

	ci.Next(context.Background())
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}
}

func TestCIteratorBackAndForth(t *testing.T) {
	// prepare the staff
	dir, err := ioutil.TempDir("", "citeratorBackAndForthTest")
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

	p.register(123, fn)
	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(123, p, &cw.lroCfrmd, &cw.sizeCfrmd, buf)

	si := inmem.SrtingsIterator("aa", "bb")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	// forth
	ctx := context.Background()
	ci.SetPos(-1)
	for i := 0; i < 3; i++ {
		res, err := ci.Get(ctx)
		if string(res) != "aa" || err != nil {
			t.Fatal("expecting aa, but got ", string(res), " and err=", err)
		}
		ci.Next(ctx)
		ci.Next(ctx)
		_, err = ci.Get(ctx)
		if err != io.EOF {
			t.Fatal("Expecting io.EOF, but got err=", err)
		}

		ci.SetBackward(true)
		res, err = ci.Get(ctx)
		if string(res) != "bb" || err != nil {
			t.Fatal("expecting bb, but got ", string(res), " and err=", err)
		}
		ci.Next(ctx)
		res, err = ci.Get(ctx)
		if string(res) != "aa" || err != nil {
			t.Fatal("expecting aa, but got ", string(res), " and err=", err)
		}
		ci.Next(ctx)
		_, err = ci.Get(ctx)
		if err != io.EOF {
			t.Fatal("Expecting io.EOF, but got err=", err)
		}
		ci.SetBackward(false)
	}
}
