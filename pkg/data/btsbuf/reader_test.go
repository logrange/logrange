package btsbuf

import (
	"fmt"
	"io"
	"testing"
)

func TestNilBuf(t *testing.T) {
	var r Reader
	err := r.Reset(nil)
	if err != nil {
		t.Fatal("Should be nil, but ", err)
	}

	_, err = r.Get()
	if err != io.EOF {
		t.Fatal("Should be io.EOF, but err=", err)
	}

	r.Next()
	_, err = r.Get()
	if err != io.EOF {
		t.Fatal("Should be io.EOF, but err=", err)
	}
}

func TestNotOk(t *testing.T) {
	testBufNotOk(t, []byte{0, 0, 0})
	testBufNotOk(t, []byte{0, 0, 0, 1, 0, 1})
	testBufNotOk(t, []byte{0, 0, 0, 1, 255, 255, 255, 255})
	testBufNotOk(t, []byte{0, 0, 0, 3, 1, 2, 3, 255, 255, 255, 254, 1, 2})
}

func TestOk(t *testing.T) {
	testBufOk(t, nil)
	testBufOk(t, []byte{0, 0, 0, 0}, []byte{})
	testBufOk(t, []byte{255, 255, 255, 255})
	testBufOk(t, []byte{0, 0, 0, 1, 123, 255, 255, 255, 255, 34}, []byte{123})
	testBufOk(t, []byte{0, 0, 0, 3, 123, 1, 2}, []byte{123, 1, 2})
	testBufOk(t, []byte{0, 0, 0, 2, 123, 1, 0, 0, 0, 0, 0, 0, 0, 1, 10}, []byte{123, 1}, []byte{}, []byte{10})
}

func testBufNotOk(t *testing.T, b []byte) {
	_, err := check(b)
	if err == nil {
		t.Fatal("expecting an err but got nil")
	}
}

func testBufOk(t *testing.T, b []byte, exp ...[]byte) {
	err := testBuf(b, exp...)
	if err != nil {
		t.Fatal("expecting err=nil, but err=", err.Error())
	}
}

func testBuf(b []byte, exp ...[]byte) error {
	var r Reader

	err := r.Reset(b)
	if err != nil {
		return err
	}

	for _, e := range exp {
		d, err := r.Get()
		if err != nil {
			return err
		}

		if !testEq(e, d) {
			return fmt.Errorf("compare %v with %v and they seem different", e, d)
		}
		r.Next()
	}

	d, err := r.Get()
	if err != io.EOF {
		return fmt.Errorf("EOF is expected, but d=%v, err=%v", d, err)
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
