package records

import (
	"fmt"
	"io"
	"testing"
)

func TestNilBuf(t *testing.T) {
	var r Reader
	err := r.Reset(nil, true)
	if err != nil {
		t.Fatal("Should be nil, but ", err)
	}

	_, err = r.Get(nil)
	if err != io.EOF {
		t.Fatal("Should be io.EOF, but err=", err)
	}

	r.Next(nil)
	_, err = r.Get(nil)
	if err != io.EOF {
		t.Fatal("Should be io.EOF, but err=", err)
	}
}

func TestOk(t *testing.T) {
	testBufOk(t, nil)
	testBufOk(t, []byte{0, 0, 0, 0}, []byte{})
	testBufOk(t, []byte{255, 255, 255, 255})
	testBufOk(t, []byte{0, 0, 0, 1, 123, 255, 255, 255, 255, 34}, []byte{123})
	testBufOk(t, []byte{0, 0, 0, 3, 123, 1, 2}, []byte{123, 1, 2})
	testBufOk(t, []byte{0, 0, 0, 2, 123, 1, 0, 0, 0, 0, 0, 0, 0, 1, 10}, []byte{123, 1}, []byte{}, []byte{10})
}

func testBufOk(t *testing.T, b []byte, exp ...[]byte) {
	err := testBuf(b, exp...)
	if err != nil {
		t.Fatal("expecting err=nil, but err=", err.Error())
	}
}

func testBuf(b []byte, exp ...[]byte) error {
	var r Reader

	err := r.Reset(b, false)
	if err != nil {
		return err
	}

	for _, e := range exp {
		d, err := r.Get(nil)
		if err != nil {
			return err
		}

		if !testEq(e, d) {
			return fmt.Errorf("compare %v with %v and they seem different", e, d)
		}
		r.Next(nil)
	}

	d, err := r.Get(nil)
	if err != io.EOF {
		return fmt.Errorf("EOF is expected, but d=%v, err=%v", d, err)
	}

	if r.Len() != len(exp) {
		return fmt.Errorf("Expected reader size is %d, but Len()=%d", len(exp), r.Len())
	}

	return nil
}

func testEq(a, b []byte) bool {

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
