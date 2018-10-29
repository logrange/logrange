package fs

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestCheckNewChunkIsOk(t *testing.T) {
	dir, err := ioutil.TempDir("", "chunkTest")
	if err != nil {
		t.Fatal("Could not create new dir err=", err)
	}
	defer os.RemoveAll(dir) // clean up
	p := NewFdPool(2)
	defer p.Close()

	cfg := &ChunkConfig{FileName: path.Join(dir, "test")}
	c, err := NewChunk(context.Background(), cfg, p)
	if err != nil {
		t.Fatal("Must be able to create file")
	}
	c.Close()
}
