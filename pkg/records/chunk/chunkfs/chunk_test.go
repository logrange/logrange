package chunkfs

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
)

func testCheckNewChunkIsOk(t *testing.T) {
	dir, err := ioutil.TempDir("", "chunkTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	p := NewFdPool(2)
	defer p.Close()

	cfg := Config{FileName: path.Join(dir, "123.dat"), MaxChunkSize: 1024}
	c, err := New(context.Background(), cfg, p)
	if err != nil {
		t.Fatal("Must be able to create file")
	}

	if c.Id() != chunk.Id(123) {
		t.Fatal("Expecting c.Id()==123, but it is ", c.Id())
	}

	// test itself
	it, _ := c.Iterator()
	_, err = it.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	si := records.SrtingsIterator("aaa", "bbb")
	n, offs, err := c.Write(context.Background(), si)
	if n != 2 || offs != 11 || err != nil {
		t.Fatal("expecting n=2, offs=11, err=nil, but n=", n, " offs=", offs, ", err=", err)
	}

	c.w.flush()
	rec, err := it.Get(context.Background())
	if err != nil || string(rec) != "aaa" {
		t.Fatal("Expecting err=nil and aaa, but err=", err, ", rec=", string(rec))
	}
	it.Next(context.Background())
	it.Next(context.Background())

	_, err = it.Get(context.Background())
	if err != io.EOF {
		t.Fatal("Expecting io.EOF, but got err=", err)
	}

	it.Close()
	c.Close()
	if len(p.frs) != 0 {
		t.Fatal("Resources are not freed properly")
	}

	// second approach
	c, err = New(context.Background(), cfg, p)
	if err != nil {
		t.Fatal("Must be able to create the chunk again")
	}

	it, _ = c.Iterator()
	it.Next(context.Background())
	rec, err = it.Get(context.Background())
	if err != nil || string(rec) != "bbb" {
		t.Fatal("Expecting err=nil and bbb, but err=", err, ", rec=", string(rec))
	}
	c.Close()
	it.Close()
}

func TestMakeChunkFileName(t *testing.T) {
	res := MakeChunkFileName("aaa", 0)
	if MakeChunkFileName("aaa", 0) != "aaa/0000000000000000.dat" {
		t.Fatal("unexpected res=", res)
	}
}

func TestSetChunkDataFileExt(t *testing.T) {
	if SetChunkDataFileExt("aaaa") != "aaaa"+ChnkDataExt {
		t.Fatal("expecting ", "aaaa"+ChnkDataExt, " but got ", SetChunkDataFileExt("aaaa"))
	}
}

func TestSetChunkIdxFileExt(t *testing.T) {
	if SetChunkIdxFileExt("aaaa"+ChnkDataExt) != "aaaa"+ChnkIndexExt {
		t.Fatal("expecting ", "aaaa"+ChnkIndexExt, " but got ", SetChunkIdxFileExt("aaaa"+ChnkDataExt))
	}
}

func testCheckPerf(t *testing.T) {
	dir, err := ioutil.TempDir("", "chunkTest22")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	fmt.Println("start at ", time.Now())
	defer os.RemoveAll(dir) // clean up

	p := NewFdPool(2)
	defer p.Close()

	cfg := Config{FileName: path.Join(dir, "123.dat"), MaxChunkSize: 1 * 1024 * 1024}
	c, err := New(context.Background(), cfg, p)
	if err != nil {
		t.Fatal("Must be able to create file, err=", err)
	}

	si := records.SrtingsIterator("aaahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhaaaa",
		"bbasjdflkjasdf;lkjasd;flkjas;dlfkjasdlkfjasldkfj;asdkfj;aksdfj;akdjf;ajdsf;kjasdflkjads;fb",
		"adsfiojaskdfjlajdflajsdflkjadslfjalsdfjl asdlfkjalsd fl aflja sfldj aldf la sdfl",
		"akdjflakjsdf lasdjf lajd fl l j").(*records.Reader)

	start := time.Now()
	cnt := 0
	for {
		n, _, err := c.Write(context.Background(), si)
		if err != nil {
			t.Log("Error err=", err)
			break
		}
		cnt += n
		si.Reset(si.Buf(), false)
	}
	diff := time.Now().Sub(start)
	fmt.Println("written ", cnt, " it took  ", diff, "1 rec write=", time.Duration(diff/time.Duration(cnt)))

	time.Sleep(time.Millisecond)

	c.w.flush()

	it, _ := c.Iterator()
	start = time.Now()
	cnt = 0
	for {
		_, err := it.Get(context.Background())
		if err != nil {
			break
		}
		cnt++
		it.Next(context.Background())
	}
	fmt.Println("read cnt=", cnt, " it took  ", time.Now().Sub(start))
	c.Close()
}
