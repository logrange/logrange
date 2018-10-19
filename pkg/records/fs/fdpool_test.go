package fs

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

func TestFdPoolClose(t *testing.T) {
	fn := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn)

	fdp := NewFdPool(1)
	fr, err := fdp.Acquire(context.Background(), fn, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	if len(fdp.frs) != 1 || len(fdp.frs[fn].rdrs) != 1 || fdp.frs[fn].rdrs[0] != fr || fdp.curSize != 1 {
		t.Fatal("Expecting file reader, but ", fdp.frs)
	}
	fdp.Close()
	time.Sleep(10 * time.Millisecond)
	if len(fdp.frs) != 0 || fdp.curSize != 0 || fr.fd != nil {
		t.Fatal("the fr is still open :( ")
	}

	_, err = fdp.Acquire(context.Background(), fn, 0)
	if err != ErrWrongState {
		t.Fatal("expecting wrong state, but ", err)
	}
}

func TestFdPoolRelease(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(2)
	defer fdp.Close()

	fr1, err := fdp.Acquire(context.Background(), fn1, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	fr2, err := fdp.Acquire(context.Background(), fn1, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	if len(fdp.frs) != 1 || len(fdp.frs[fn1].rdrs) != 2 {
		t.Fatal("expecting 2 pools")
	}

	fdp.Release(fr2)
	if fr2.plState != cFrsFree || len(fdp.frs[fn1].rdrs) != 1 {
		t.Fatal("Must be cleaned up because of hitting limits ")
	}

	fdp.Release(fr1)
	if fr1.plState != cFrsFree || len(fdp.frs[fn1].rdrs) != 1 {
		t.Fatal("fr1 must stay in the pool ")
	}
}

func TestFdPoolOverflow(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(1)
	defer fdp.Close()

	fr1, err := fdp.Acquire(context.Background(), fn1, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	start := time.Now()
	go func(fr *fReader) {
		time.Sleep(50 * time.Millisecond)
		fdp.Release(fr)
	}(fr1)

	_, err = fdp.Acquire(context.Background(), fn1, 0)
	if time.Now().Sub(start) < time.Duration(50*time.Millisecond) {
		t.Fatal("It took less than expected. Should be blocked.")
	}

	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}
}

func TestFdPoolInOverflowCycling(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(1)
	defer fdp.Close()

	fr1, err := fdp.Acquire(context.Background(), fn1, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	for i := 0; i < 100; i++ {
		go func(fr *fReader) {
			time.Sleep(time.Millisecond)
			fdp.Release(fr)
		}(fr1)

		fr1, err = fdp.Acquire(context.Background(), fn1, 0)
		if err != nil {
			t.Fatal("Could not acquire context ", err)
		}
	}
}

func TestFdPoolGetFree(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(3)
	defer fdp.Close()

	fr1, _ := fdp.Acquire(context.Background(), fn1, 0)
	fr2, _ := fdp.Acquire(context.Background(), fn1, 0)

	fdp.Release(fr1)
	fdp.Release(fr2)

	fr1.pos = 100
	fr2.pos = 200

	fr, _ := fdp.Acquire(context.Background(), fn1, 201)
	if fr != fr2 {
		t.Fatal("Expecting fr2, but got ", fr)
	}
	fdp.Release(fr)

	fr, _ = fdp.Acquire(context.Background(), fn1, 199)
	if fr != fr1 {
		t.Fatal("Expecting fr2, but got ", fr1)
	}
	fdp.Release(fr)
}

func TestFdPoolAcquireClosedCtx(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(1)
	defer fdp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	fdp.Acquire(context.Background(), fn1, 201)

	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := fdp.Acquire(ctx, fn1, 201)
	if err == nil || err != ctx.Err() || time.Now().Sub(start) < time.Duration(50*time.Microsecond) {
		t.Fatal("Must be error=", ctx.Err())
	}

}

func removeTmpFileAndDir(fn string) {
	dir := path.Dir(fn)
	os.RemoveAll(dir)
}

func createTmpDirAndFile(t *testing.T, fn string) string {
	dir, err := ioutil.TempDir("", "tmpTestFdPool")
	if err != nil {
		t.Fatal("Could not create tempory dir ", err)
	}

	fn = path.Join(dir, fn)
	_, err = os.Create(fn)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal("Could not create filename=", fn)
	}
	return fn
}
