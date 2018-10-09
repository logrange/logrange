package inmem

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
)

func TestWriterEmpty(t *testing.T) {
	var bbw Writer
	var buf [100]byte
	bbw.Reset(buf[:], false)
	bf, err := bbw.Close()
	if err != nil || len(bf) != 0 {
		t.Fatal("should be empty")
	}

	bf, err = bbw.Close()
	if err != nil || len(bf) != 0 {
		t.Fatal("should be empty from second attempt")
	}
}

func TestWriterAllocate(t *testing.T) {
	var bbw Writer
	var buf [100]byte
	bbw.Reset(buf[:], false)
	bf, err := bbw.Allocate(20, true)
	if bf == nil || len(bf) != 20 || err != nil {
		t.Fatal("Should be able to allocate 20 bytes err=", err)
	}

	bf, err = bbw.Allocate(20, true)
	if bf == nil || len(bf) != 20 || err != nil {
		t.Fatal("Should be able to allocate 20 bytes again err=", err)
	}

	bf, err = bbw.Allocate(60, true)
	if err == nil {
		t.Fatal("Should not be able to allocate 60 bytes err=", err)
	}

	if bbw.offs != 48 {
		t.Fatal("expecting offs=48, but it is ", bbw.offs)
	}

	bf, err = bbw.Close()
	if err != nil || len(bf) != 48 {
		t.Fatal("should be 48 bytes lenght")
	}

	v := binary.BigEndian.Uint32(buf[48:])
	if v != 0xFFFFFFFF {
		t.Fatal("No marker")
	}
}

func TestWriterAllocateExt(t *testing.T) {
	var bbw Writer
	bbw.Reset(nil, true)
	for i := 0; i < 100; i++ {
		_, err := bbw.Allocate(0, true)
		if err != nil {
			t.Fatal("Should be extendable err=", err)
		}
	}
	if len(bbw.buf) < 400 {
		t.Fatal("Expecting at least 400 bytes in length, but it is ", len(bbw.buf))
	}

	bbw.Reset(nil, true)
	for i := 0; i < 100; i++ {
		_, err := bbw.Allocate(100, true)
		if err != nil {
			t.Fatal("Should be extendable err=", err)
		}
	}
	if len(bbw.buf) < 10400 {
		t.Fatal("Expecting at least 10400 bytes in length, but it is ", len(bbw.buf))
	}
}

func TestWriterAllocateExt2(t *testing.T) {
	var bbw Writer
	var buf [10]byte
	bbw.Reset(buf[:], true)
	_, err := bbw.Allocate(10, false)
	if err == nil {
		t.Fatal("Should report error, cause ask to not extend")
	}

	bf, err := bbw.Allocate(10, true)
	if err != nil || len(bf) != 10 {
		t.Fatal("Should not report error, cause ask to extend")
	}

	bbw.Reset(buf[:], false)
	_, err = bbw.Allocate(10, true)
	if err == nil {
		t.Fatal("Should report error, cause ask to extend, but it is not extendable")
	}
}

func TestInsufficientAllocate(t *testing.T) {
	var bbw Writer
	var buf [12]byte
	bbw.Reset(buf[:], false)
	bf, err := bbw.Allocate(4, true)
	if bf == nil || len(bf) != 4 || err != nil {
		t.Fatal("Should be able to allocate 4 bytes err=", err)
	}

	bbw.Reset(buf[:], false)
	bf, err = bbw.Allocate(2, true)
	if bf == nil || len(bf) != 2 || err != nil {
		t.Fatal("Should be able to allocate 2 bytes err=", err)
	}

	bbw.Reset(buf[:], false)
	bf, err = bbw.Allocate(7, true)
	if bf == nil || len(bf) != 7 || err != nil {
		t.Fatal("Should be able to allocate 7 bytes err=", err)
	}

	bbw.Reset(buf[:], false)
	bf, err = bbw.Allocate(8, true)
	if bf == nil || len(bf) != 8 || err != nil {
		t.Fatal("Should be able to allocate 8 bytes err=", err)
	}

	bbw.Reset(buf[:], false)
	bf, err = bbw.Allocate(9, true)
	if bf != nil || err == nil {
		t.Fatal("Should not be able to allocate 9 bytes err=", err)
	}
	bbw.Reset(buf[:], false)
	bf, err = bbw.Allocate(25, true)
	if bf != nil || err == nil {
		t.Fatal("Should not be able to allocate 25 bytes err=", err)
	}
}

func TestAllocateAndClosed(t *testing.T) {
	var bbw Writer
	var buf [12]byte
	bbw.Reset(buf[:], false)
	bbw.Allocate(6, true)
	bf, err := bbw.Close()
	if len(bf) != 10 || len(bbw.buf) != 10 || err != nil {
		t.Fatal("Should be closed ok without marker! err=", err)
	}

	bbw.Reset(bbw.buf, false)
	if len(bbw.buf) != 12 {
		t.Fatal("Buf len must be set to capacity")
	}
	bf, err = bbw.Close()
	if len(bf) != 0 || len(bbw.buf) != 12 || err != nil {
		t.Fatal("Should be closed ok without marker! err=", err)
	}
	_, err = bbw.Allocate(2, true)
	if err == nil {
		t.Fatal("Allocate must return error after closing")
	}
}

func TestResetReader(t *testing.T) {
	var bbi Reader
	var buf [20]byte
	err := bbi.Reset(buf[:4])
	if err != nil {
		t.Fatal("should not be problem with empty reset, err=", err)
	}
	err = bbi.Reset(buf[:])
	if err != nil {
		t.Fatal("should not be problem with empty reset(2), err=", err)
	}

	err = bbi.Reset(buf[:8])
	if err != nil {
		t.Fatal("should not be problem with empty reset(3), err=", err)
	}

	err = bbi.Reset(buf[:7])
	if err == nil {
		t.Fatal("should be problem with empty reset, but it is not")
	}
	err = bbi.Reset(buf[:2])
	if err == nil {
		t.Fatal("should be problem with empty reset, but it is not")
	}

	binary.BigEndian.PutUint32(buf[:], 3)
	binary.BigEndian.PutUint32(buf[7:], 0xFFFFFFFF)
	err = bbi.Reset(buf[:])
	if err != nil {
		t.Fatal("should not be problem with empty reset(4), err=", err)
	}

	binary.BigEndian.PutUint32(buf[7:], 0x12FFFFFF)
	err = bbi.Reset(buf[:])
	if err == nil {
		t.Fatal("should be problem with empty reset, but it is not (2)")
	}
}

func TestReader(t *testing.T) {
	var bbw Writer
	var src [92]byte

	rand.Read(src[:])
	bbw.Reset(nil, true)
	var sz int
	for offs := 0; offs < len(src); offs += sz {
		sz = 20 + offs/10
		bw, err := bbw.Allocate(sz, true)
		if len(bw) != sz || err != nil {
			t.Fatal("Something goes wrong with allocation err=", err)
		}
		copy(bw, src[offs:])
	}

	bw, err := bbw.Close()
	if len(bw) != 108 || err != nil {
		t.Fatal("Something goes wrong with writing")
	}

	var bbi Reader
	err = bbi.Reset(bbw.buf)
	if err != nil {
		t.Fatal("Expecting no problems with the dst buf, but err=", err)
	}

	if bbi.Len() != 4 {
		t.Fatal("Expected len is 4, but got ", bbi.Len())
	}

	offs := 0
	r, err := bbi.Get()
	for err == nil {
		sz = 20 + offs/10
		if len(r) != sz {
			t.Fatal("Wrong buf size ", len(r), ", but expected ", sz)
		}
		if !reflect.DeepEqual([]byte(r), src[offs:offs+sz]) {
			t.Fatal("wrong data read. Expected size sz=", sz, ", offs=", offs)
		}
		offs += sz
		bbi.Next()
		r, err = bbi.Get()
	}
}

func TestReaderEven(t *testing.T) {
	var bbw Writer
	var dst [16]byte
	var src [8]byte

	rand.Read(src[:])
	bbw.Reset(dst[:], false)
	for offs := 0; offs < len(src); offs += 4 {
		bw, err := bbw.Allocate(4, true)
		if len(bw) != 4 || err != nil {
			t.Fatal("Something goes wrong with allocation err=", err)
		}
		copy(bw, src[offs:])
	}

	bw, err := bbw.Close()
	if len(bw) != 16 || err != nil {
		t.Fatal("Something goes wrong with writing")
	}

	var bbi Reader
	err = bbi.Reset(dst[:])
	if err != nil {
		t.Fatal("Expecting no problems with the dst buf, but err=", err)
	}
	if bbi.Len() != 2 {
		t.Fatal("Expected len is 2, but got ", bbi.Len())
	}

	offs := 0
	its := 0
	r, err := bbi.Get()
	for err == nil {
		its++
		if len(r) != 4 {
			t.Fatal("Wrong buf size ", len(r), ", but expected 4")
		}
		if !reflect.DeepEqual([]byte(r), src[offs:offs+4]) {
			t.Fatal("wrong data read. Expected size 4, offs=", offs)
		}
		offs += 4
		bbi.Next()
		r, err = bbi.Get()
	}

	if its != 2 {
		t.Fatal("should be iterate over 2 elems")
	}
}

func TestBrokenReaderReset(t *testing.T) {
	var bbw Writer
	var buf [100]byte
	bbw.Reset(buf[:], false)
	bbw.Allocate(10, true)
	bbw.Allocate(20, true)
	bbw.Close()

	var bbr Reader
	bbr.Reset(bbw.buf)
	if bbr.End() || bbr.Len() != 2 {
		t.Fatal("the buffer must be reset ok")
	}

	buf[1] = 0xFF
	err := bbr.Reset(buf[:])
	if err == nil || !bbr.End() || bbr.Len() != 0 {
		t.Fatal("the buffer must not be reset, but err=", err, " bbr.End()=", bbr.End(), ", bbr.Len()=", bbr.Len())
	}
}
