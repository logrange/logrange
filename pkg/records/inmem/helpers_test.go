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

func TestByteArrayQuickCasts(t *testing.T) {
	b := make([]byte, 10)
	s := ByteArrayToString(b)
	b[0] = 'a'
	if len(s) != 10 || s[0] != 'a' {
		t.Fatal("must be pointed to same object s=", s, " b=", b)
	}

	s = "Hello WOrld"
	bf := StringToByteArray(string(s))
	s2 := ByteArrayToString(bf)
	if s != s2 {
		t.Fatal("Oops, expecting s1=", s, ", but really s2=", s2)
	}

	bf = StringToByteArray("")
	s2 = ByteArrayToString(bf)
	if s2 != "" {
		t.Fatal("Oops, expecting empty string, but got s2=", s2)
	}
}
