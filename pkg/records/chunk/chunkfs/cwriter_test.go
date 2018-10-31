package chunkfs

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"
)

type lazyIt struct {
	buf   records.Record
	delay time.Duration
	limit int
}

func (li *lazyIt) Next(ctx context.Context) {
	li.limit--
}

func (li *lazyIt) Get(ctx context.Context) (records.Record, error) {
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

	cw := newCWriter(path.Join(dir, "tst"), -1, 0, 1000)
	defer cw.Close()

	flushes := int32(0)
	cw.onFlushF = func() {
		atomic.AddInt32(&flushes, 1)
	}

	cw.flushTO = 50 * time.Millisecond
	if cw.isFlushNeeded() {
		t.Fatal("Should not be flush needed")
	}

	si := records.SrtingsIterator("a")
	n, offs, err := cw.write(nil, si)
	if n != 1 || offs != 0 || err != nil || atomic.LoadInt32(&flushes) != 0 {
		t.Fatal("Expecting n=1, offs=0, err=nil, but n=", n, ", offs=", offs, ", err=", err)
	}
	w := cw.w
	cw.closeFWriterUnsafe()
	time.Sleep(10 * time.Millisecond)
	if cw.lro != cw.lroCfrmd || cw.lro != 0 || atomic.LoadInt32(&flushes) != 1 {
		t.Fatal("expecting lro=0, but it is ", cw.lro, "flushes=", flushes)
	}

	si = records.SrtingsIterator("a", "b", "c")
	n, offs, err = cw.write(nil, si)
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
	if cw.isFlushNeeded() || atomic.LoadInt32(&flushes) != 2 {
		t.Fatal("Must be flushed")
	}
}

func TestCWriterIdleTimeout(t *testing.T) {
	dir, err := ioutil.TempDir("", "cwriterWriteTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cw := newCWriter(path.Join(dir, "tst"), -1, 0, 1000)
	defer cw.Close()

	cw.idleTO = 50
	cw.flushTO = 10

	si := records.SrtingsIterator("a")
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

	cw := newCWriter(path.Join(dir, "tst"), -1, 0, 1000)
	defer cw.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cw.Close()
	}()

	n, _, err := cw.write(nil, &lazyIt{records.Record([]byte{65}), 20 * time.Millisecond, 100})
	if n < 1 || n > 4 || err != util.ErrWrongState {
		t.Fatal("expecting low n < 4 and err== ErrWrongState, but n=", n, " and the err=", err)
	}
}

func TestCWriterMaxSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "cwriterMaxSize")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	cw := newCWriter(path.Join(dir, "tst"), -1, 0, 1)
	defer cw.Close()

	si := records.SrtingsIterator("a", "b", "c")
	n, offs, err := cw.write(nil, si)
	if n != 3 || offs != 18 || err != nil {
		t.Fatal("Expecting n=3, offs=27, err=nil, but n=", n, ", offs=", offs, ", err=", err)
	}

	si = records.SrtingsIterator("a", "b", "c")
	_, _, err = cw.write(nil, si)
	if err != util.ErrMaxSizeReached {
		t.Fatal("Must report ErrMaxSizeReached")
	}
}
