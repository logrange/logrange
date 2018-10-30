package fs

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records/inmem"
)

func TestCCheckerCreateFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "cCheckerCreateFileTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "test")
	p := NewFdPool(2)
	defer p.Close()

	cc := cChecker{fileName: fn, fdPool: p, logger: log4g.GetLogger("cChecker")}

	err = cc.checkFileConsistency(context.Background(), 0)
	if err != nil {
		t.Fatal("Unexpected error while checking the file consistency err=", err)
	}
	if cc.lro != -1 || cc.iSize != 0 || cc.tSize != 0 {
		t.Fatal("unexpected params. Expecting lro=-1, iSize == tSize == 0, but lro=", cc.lro, ", iSize=", cc.iSize, ", tSize=", cc.tSize)
	}

	// Check the file was created
	_, err = os.Stat(fn)
	if err != nil {
		t.Fatal("file must exist, but err=", err)
	}

	// Pool should not be touched, because of the new file
	if p.curSize != 0 {
		t.Fatal("Wrong state of the pool ", p)
	}
}

func TestCCheckerNormalFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "cCheckerCreateFileTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "test")
	p := NewFdPool(2)
	defer p.Close()

	cc := cChecker{fileName: fn, fdPool: p, logger: log4g.GetLogger("cChecker")}
	lro := testCCheckerTestFile(t, fn, []string{"aaa", "bbb", "ccc"})

	// Check the file was created
	fi, err := os.Stat(fn)
	if err != nil {
		t.Fatal("file must exist, but err=", err)
	}

	// quick scan
	err = cc.checkFileConsistency(context.Background(), 0)
	if err != nil {
		t.Fatal("Unexpected error while checking the file consistency err=", err)
	}
	if cc.lro != lro || cc.iSize != fi.Size() || cc.tSize != fi.Size() {
		t.Fatal("unexpected params. Expecting lro=", lro, ", iSize == tSize == ", fi.Size(), ", but lro=", cc.lro, ", iSize=", cc.iSize, ", tSize=", cc.tSize)
	}

	// full scan
	err = cc.checkFileConsistency(context.Background(), ChnkChckFullScan)
	if err != nil {
		t.Fatal("Unexpected error while checking the file consistency err=", err)
	}
	if cc.lro != lro || cc.iSize != fi.Size() || cc.tSize != fi.Size() {
		t.Fatal("unexpected params. Expecting lro=", lro, ", iSize == tSize == ", fi.Size(), ", but lro=", cc.lro, ", iSize=", cc.iSize, ", tSize=", cc.tSize)
	}

	// check the pool
	if p.curSize != 1 || p.frs[fn].rdrs[0].plState != 0 {
		t.Fatal("something wrong with the pool state")
	}
}

func TestCCheckerCorruptedFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "cCheckerCreateFileTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up

	fn := path.Join(dir, "test")
	p := NewFdPool(2)
	defer p.Close()

	cc := cChecker{fileName: fn, fdPool: p, logger: log4g.GetLogger("cChecker")}
	testCCheckerTestFile(t, fn, []string{"a", "b", "c"})

	// Check the file was created
	fi, err := os.Stat(fn)
	if err != nil {
		t.Fatal("file must exist, but err=", err)
	}
	os.Truncate(fn, fi.Size()-2)

	// check corrupted file
	err = cc.checkFileConsistency(context.Background(), 0)
	if err == nil {
		t.Fatal("Expecting error, but got err=nil")
	}

	// check with full-scan, but no truncation
	err = cc.checkFileConsistency(context.Background(), ChnkChckFullScan)
	if err != nil {
		t.Fatal("Expecting no erro, but err=", err)
	}
	if cc.lro != 9 || cc.iSize != 25 || cc.tSize != 18 {
		t.Fatal("unexpected params. Expecting lro=9, iSize=25, tSize=18, but lro=", cc.lro, ", iSize=", cc.iSize, ", tSize=", cc.tSize)
	}
	fi, _ = os.Stat(fn)
	if fi.Size() != 25 {
		t.Fatal("file must not be truncated(size should be 25), but size=", fi.Size())
	}

	// check with truncation enabled
	err = cc.checkFileConsistency(context.Background(), ChnkChckTruncateOk)
	if err != nil {
		t.Fatal("Unexpected error while checking the file consistency err=", err)
	}
	if cc.lro != 9 || cc.iSize != 25 || cc.tSize != 18 {
		t.Fatal("unexpected params. Expecting lro=9, iSize=25, tSize=18, but lro=", cc.lro, ", iSize=", cc.iSize, ", tSize=", cc.tSize)
	}
}

func testCCheckerTestFile(t *testing.T, fn string, data []string) int64 {
	cw := newCWriter(fn, -1, 0, 1000)
	defer cw.Close()

	si := inmem.SrtingsIterator(data...)
	n, lro, err := cw.write(nil, si)
	if n != len(data) || err != nil {
		t.Fatal("Expecting n=", len(data), ", err=nil, but n=", n, ", err=", err)
	}
	return lro
}
