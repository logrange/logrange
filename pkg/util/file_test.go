package util

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFileId(t *testing.T) {
	fd, err := ioutil.TempFile("/tmp", "GetFileId_")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if fd != nil {
			fd.Close()
			os.Remove(fd.Name())
		}
	}()

	fi, err := fd.Stat()
	if err != nil {
		t.Fatal(err)
	}

	id := GetFileId(fd.Name(), fi)
	pp := strings.Split(id, "_")

	hsh := pp[0]
	ino, _ := strconv.ParseUint(pp[1], 10, 64)
	dev, _ := strconv.ParseUint(pp[2], 10, 32)

	assert.Equal(t, len(pp), 3)
	assert.Equal(t, hsh, Md5(fd.Name()))
	assert.Equal(t, ino, fi.Sys().(*syscall.Stat_t).Ino)
	assert.Equal(t, uint64(dev), uint64(fi.Sys().(*syscall.Stat_t).Dev))
}

func TestSetFileExt(t *testing.T) {
	if s := SetFileExt("/a/b/c.ddd", ".idx"); "/a/b/c.idx" != s {
		t.Fatal("expecting \"/a/b/c.idx\" but got ", s)
	}

	if s := SetFileExt("/a/b/c.ddd", "idx"); "/a/b/c.idx" != s {
		t.Fatal("expecting \"/a/b/c.idx\" but got ", s)
	}
	if s := SetFileExt("abcd.txt", ""); "abcd" != s {
		t.Fatal("expecting \"abcd\" but got ", s)
	}
}
