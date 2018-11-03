package chunkfs

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"
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
	cw := newCWriter(fn, 0, 1000, 0)
	cw.ensureFWriter() // to create the file
	defer cw.Close()

	p.register(123, frParams{fn, ChnkIdxReaderBufSize})
	p.register(124, frParams{util.SetFileExt(fn, ChnkIndexExt), ChnkIdxReaderBufSize})
	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(123, p, &cw.cntCfrmd, buf)

	// empty it
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}
	ci.Next(context.Background())

	si := records.SrtingsIterator("aa")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err := ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	res, err = ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	ci.Release()
	res, err = ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	ci.Next(context.Background())
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	// wrong offset
	ci.SetPos(2)
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting ErrCorruptedData, but got err=", err)
	}

	si = records.SrtingsIterator("bb")
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
	if string(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", string(res), " and err=", err)
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
	if records.ByteArrayToString(res) != "bb" || err != nil {
		t.Fatal("expecting bb, but got ", records.ByteArrayToString(res), " and err=", err)
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
	cw := newCWriter(fn, 0, 1000, 0)
	cw.ensureFWriter() // to create the file
	defer cw.Close()

	p.register(123, frParams{fn, ChnkIdxReaderBufSize})
	p.register(124, frParams{util.SetFileExt(fn, ChnkIndexExt), ChnkIdxReaderBufSize})
	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(123, p, &cw.cntCfrmd, buf)

	si := records.SrtingsIterator("aa")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err := ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	ci.SetPos(0)
	res, err = ci.Get(context.Background())
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}

	// wrong offset
	ci.SetPos(5)
	res, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err, "res=", string(res), " pos=", ci.Pos())
	}

	ci.SetPos(1235)
	_, err = ci.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	si = records.SrtingsIterator("bb")
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
	cw := newCWriter(fn, 0, 1000, 0)
	cw.ensureFWriter() // to create the file
	defer cw.Close()

	p.register(123, frParams{fn, ChnkIdxReaderBufSize})
	p.register(124, frParams{util.SetFileExt(fn, ChnkIndexExt), ChnkIdxReaderBufSize})
	buf := make([]byte, 10)

	// test it now
	ci := newCIterator(123, p, &cw.cntCfrmd, buf)

	si := records.SrtingsIterator("aa", "bb")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	// forth
	ctx := context.Background()
	ci.SetPos(0)
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

		ci.SetPos(1)
		res, err = ci.Get(ctx)
		if string(res) != "bb" || err != nil {
			t.Fatal("expecting bb, but got ", string(res), " and err=", err)
		}
		ci.Release()
		res, err = ci.Get(ctx)
		if string(res) != "bb" || err != nil {
			t.Fatal("expecting bb, but got ", string(res), " and err=", err)
		}
		ci.Release()
		ci.Next(ctx)
		_, err = ci.Get(ctx)
		if err != io.EOF {
			t.Fatal("Expecting io.EOF, but got err=", err)
		}
		ci.SetPos(0)
	}
}

func TestCIteratorEmpty(t *testing.T) {
	// prepare the staff
	dir, err := ioutil.TempDir("", "citeratorBackAndForthTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	p := NewFdPool(2)
	defer p.Close()

	fn := path.Join(dir, "tst")
	cw := newCWriter(fn, 0, 1000, 0)
	cw.ensureFWriter() // to create the file
	defer cw.Close()

	p.register(123, frParams{fn, ChnkIdxReaderBufSize})
	p.register(124, frParams{util.SetFileExt(fn, ChnkIndexExt), ChnkIdxReaderBufSize})
	buf := make([]byte, 10)

	// test it now
	ctx := context.Background()
	ci := newCIterator(123, p, &cw.cntCfrmd, buf)
	_, err = ci.Get(ctx)
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	_, err = ci.Get(ctx)
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	si := records.SrtingsIterator("aa", "bb")
	_, _, err = cw.write(nil, si)
	if err != nil {
		t.Fatal("could not write data to file ", fn, ", err=", err)
	}
	cw.flush()

	res, err := ci.Get(ctx)
	if string(res) != "aa" || err != nil {
		t.Fatal("expecting aa, but got ", string(res), " and err=", err)
	}
	ci.Close()
}
