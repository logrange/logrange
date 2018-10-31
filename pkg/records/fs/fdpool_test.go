package fs

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/util"
)

func TestFdPoolClose(t *testing.T) {
	fn := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn)

	fdp := NewFdPool(1)
	fdp.register(0, fn)
	fr, err := fdp.acquire(context.Background(), 0, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	if len(fdp.frs) != 1 || len(fdp.frs[0].rdrs) != 1 || fdp.frs[0].rdrs[0] != fr || fdp.curSize != 1 {
		t.Fatal("Expecting file reader, but ", fdp.frs)
	}
	fdp.Close()
	time.Sleep(10 * time.Millisecond)
	if len(fdp.frs) != 0 || fdp.curSize != 0 || fr.fd == nil {
		t.Fatal("the fr is closed, must be not!")
	}

	fdp.release(fr)
	if len(fdp.frs) != 0 || fdp.curSize != 0 || fr.fd != nil {
		t.Fatal("the fr must be closed for now!")
	}

	_, err = fdp.acquire(context.Background(), 0, 0)
	if err != util.ErrWrongState {
		t.Fatal("expecting wrong state, but ", err)
	}
}

func TestFdPoolRelease(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(2)
	fdp.register(0, fn1)
	defer fdp.Close()

	fr1, err := fdp.acquire(context.Background(), 0, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	fr2, err := fdp.acquire(context.Background(), 0, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	if len(fdp.frs) != 1 || len(fdp.frs[0].rdrs) != 2 {
		t.Fatal("expecting 2 pools")
	}

	fdp.release(fr2)
	if fr2.plState != frStateClosed || len(fdp.frs[0].rdrs) != 1 {
		t.Fatal("Must be cleaned up because of hitting limits ")
	}

	fdp.release(fr1)
	if !fr1.isFree() || len(fdp.frs[0].rdrs) != 1 {
		t.Fatal("fr1 must stay in the pool ")
	}
}

func TestFdPoolOverflow(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(1)
	fdp.register(0, fn1)
	defer fdp.Close()

	fr1, err := fdp.acquire(context.Background(), 0, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	start := time.Now()
	go func(fr *fReader) {
		time.Sleep(50 * time.Millisecond)
		fdp.release(fr)
	}(fr1)

	_, err = fdp.acquire(context.Background(), 0, 0)
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
	fdp.register(0, fn1)
	defer fdp.Close()

	fr1, err := fdp.acquire(context.Background(), 0, 0)
	if err != nil {
		t.Fatal("Could not acquire context ", err)
	}

	for i := 0; i < 100; i++ {
		go func(fr *fReader) {
			time.Sleep(time.Millisecond)
			fdp.release(fr)
		}(fr1)

		fr1, err = fdp.acquire(context.Background(), 0, 0)
		if err != nil {
			t.Fatal("Could not acquire context ", err)
		}
	}
}

func TestFdPoolGetFree(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(3)
	fdp.register(0, fn1)
	defer fdp.Close()

	fr1, _ := fdp.acquire(context.Background(), 0, 0)
	fr2, _ := fdp.acquire(context.Background(), 0, 0)

	fdp.release(fr1)
	fdp.release(fr2)

	fr1.pos = 100
	fr2.pos = 200

	fr, _ := fdp.acquire(context.Background(), 0, 201)
	if fr != fr2 {
		t.Fatal("Expecting fr2, but got ", fr)
	}
	fdp.release(fr)

	fr, _ = fdp.acquire(context.Background(), 0, 199)
	if fr != fr1 {
		t.Fatal("Expecting fr2, but got ", fr1)
	}
	fdp.release(fr)
}

func TestFdPoolacquireClosedCtx(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(1)
	fdp.register(0, fn1)
	defer fdp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	fdp.acquire(context.Background(), 0, 201)

	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := fdp.acquire(ctx, 0, 201)
	if err == nil || err != ctx.Err() || time.Now().Sub(start) < time.Duration(50*time.Microsecond) {
		t.Fatal("Must be error=", ctx.Err())
	}
}

func TestFdPoolWrongFile(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	removeTmpFileAndDir(fn1)

	fdp := NewFdPool(1)
	fdp.register(0, fn1)
	defer fdp.Close()

	_, err := fdp.acquire(context.Background(), 0, 201)
	if err == nil || len(fdp.frs) != 1 || len(fdp.frs[0].rdrs) != 0 {
		t.Fatal("Must be error! frp=", fdp)
	}
}

func TestFdPoolRegisterUnregister(t *testing.T) {
	fn1 := createTmpDirAndFile(t, "t1")
	defer removeTmpFileAndDir(fn1)

	fdp := NewFdPool(2)
	defer fdp.Close()

	_, err := fdp.acquire(context.Background(), 0, 201)
	if err == nil || len(fdp.frs) != 0 {
		t.Fatal("Must be error! frp=", fdp)
	}

	err = fdp.register(0, fn1)
	if err != nil {
		t.Fatal("Expecting err=nil, but err=", err)
	}

	err = fdp.register(0, fn1)
	if err == nil {
		t.Fatal("Expecting err!=nil, but err=nil")
	}

	fr, err := fdp.acquire(context.Background(), 0, 201)
	if err != nil || len(fdp.frs) != 1 {
		t.Fatal("Must be error! frp=", fdp)
	}

	fdp.releaseAllByCid(0)
	if len(fdp.frs) != 0 {
		t.Fatal("Must be empty now! ", fdp)
	}
	if fr.plState == frStateClosed {
		t.Fatal("not expected state frStateClosed")
	}
	fdp.release(fr)
	if fr.plState != frStateClosed {
		t.Fatal("expected state frStateClosed, but ", fr.plState)
	}

	fn2 := fn1 + "a"
	fdp.register(0, fn1)
	fdp.register(1, fn2)
	if len(fdp.frs) != 2 {
		t.Fatal("Must be 2 frs, but frp=", fdp)
	}
	fdp.releaseAllByCid(0)
	if len(fdp.frs) != 1 {
		t.Fatal("Must be 1 frs, but frp=", fdp)
	}
	fdp.releaseAllByCid(1)
	if len(fdp.frs) != 0 {
		t.Fatal("Must be 0 frs, but frp=", fdp)
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
