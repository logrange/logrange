package inmem

import (
	"io"
	"testing"
)

func TestSrtingsIterator(t *testing.T) {
	it := SrtingsIterator()
	if _, err := it.Get(); err != io.EOF {
		t.Fatal("should be EOF")
	}

	strs := []string{"aaa", "bbb", "ccc"}
	it = SrtingsIterator(strs...)
	cnt := 0
	for {
		rec, err := it.Get()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal("Unexpected error ", err)
		}

		if string(rec) != strs[cnt] {
			t.Fatal("Expected ", strs[cnt], " but got ", string(rec))
		}
		cnt++
		it.Next()
	}

	if cnt != len(strs) {
		t.Fatal("cnt must be ", len(strs), " but it is ", cnt)
	}

}
