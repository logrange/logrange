package fs

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/inmem"
)

type lazyIt struct {
	buf   records.Record
	delay time.Duration
	limit int
}

func (li *lazyIt) Next() {
	li.limit--
}

func (li *lazyIt) Get() (records.Record, error) {
	if li.limit <= 0 {
		return nil, io.EOF
	}
	time.Sleep(li.delay)
	return li.buf, nil
}

func TestCWriterWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "cwriterWriteTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cw := newCWriter(path.Join(dir, "tst"), 0)
	defer cw.Close()

	cw.flushTO = 50 * time.Millisecond
	if cw.isFlushNeeded() {
		t.Fatal("Should not be flush needed")
	}

	si := inmem.SrtingsIterator("a")
	n, offs, err := cw.write(nil, si)
	if n != 1 || offs != 0 || err != nil {
		t.Fatal("Expecting n=1, offs=0, err=nil, but n=", n, ", offs=", offs, ", err=", err)
	}
	w := cw.w
	cw.closeFWriter()
	if cw.lro != cw.lroCfrmd || cw.lro != 0 {
		t.Fatal("expecting lro=0, but it is ", cw.lro)
	}

	si = inmem.SrtingsIterator("a", "b", "c")
	n, offs, err = cw.write(si)
	if !cw.isFlushNeeded() {
		t.Fatal("expecting flush is needed")
	}
	if n != 3 || offs != 27 || err != nil {
		t.Fatal("Expecting n=3, offs=27, err=nil, but n=", n, ", offs=", offs, ", err=", err)
	}

	if cw.w == w {
		t.Fatal("Must be new writer!")
	}
	time.Sleep(60 * time.Millisecond)
	if cw.isFlushNeeded() {
		t.Fatal("Must be flushed")
	}
}

func TestCWriterIdleTimeout(t *testing.T) {
	dir, err := ioutil.TempDir("", "cwriterWriteTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cw := newCWriter(path.Join(dir, "tst"), 0)
	defer cw.Close()

	cw.idleTO = 50
	cw.flushTO = 10

	si := inmem.SrtingsIterator("a")
	cw.write(nil, si)
	time.Sleep(70 * time.Millisecond)
	if cw.w != nil {
		t.Fatal("Expecting the fWriter closed by idle timeout")
	}

	if cw.closed != 0 {
		t.Fatal("Expecting cWrter is still be opened")
	}
}

func TestCWriterCloseWhileLazyIterator(t *testing.T) {
	dir, err := ioutil.TempDir("", "cwriterWriteTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cw := newCWriter(path.Join(dir, "tst"), 0)
	defer cw.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cw.Close()
	}()

	n, _, err := cw.write(nil, &lazyIt{records.Record([]byte{65}), 20 * time.Millisecond, 100})
	if n < 1 || n > 4 || err != ErrWrongState {
		t.Fatal("expecting low n < 4 and err== ErrWrongState, but n=", n, " and the err=", err)
	}
}
