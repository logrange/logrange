package inmem

import (
	"io"
	"reflect"
	"unsafe"

	"github.com/logrange/logrange/pkg/records"
)

// SrtingsIterator receives strings and return an iterator over them. The
// function is not fast, uses many allocations and should be used in test code
// only.
func SrtingsIterator(strs ...string) records.Iterator {
	var bbw Writer
	for _, s := range strs {
		bs := []byte(s)
		bf, err := bbw.Allocate(len(bs), true)
		if err != nil {
			panic(err)
		}
		copy(bf, bs)
	}

	res, err := bbw.Close()
	if err != nil {
		panic(err)
	}

	rdr := new(Reader)
	rdr.Reset(res, false)
	return rdr
}

// ReadBufAsStringSlice receives a records buffer, iterates it over the buffer
// and returns the records as a slice of strings
func ReadBufAsStringSlice(buf Records) ([]string, error) {
	res := make([]string, 0, 10)
	var rdr Reader
	err := rdr.Reset(buf, true)
	if err != nil {
		return nil, err
	}

	for {
		r, err := rdr.Get()
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		res = append(res, string(r))
		rdr.Next()
	}
	return res, nil
}

// StringToByteArray gets a string and turn it to []byte without extra memoy allocations
//
// NOTE! Using this function is extremely dangerous, so it can be done with
// extra care with clear understanding how it works
func StringToByteArray(v string) []byte {
	var slcHdr reflect.SliceHeader
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&v))
	slcHdr.Data = sh.Data
	slcHdr.Cap = sh.Len
	slcHdr.Len = sh.Len
	return *(*[]byte)(unsafe.Pointer(&slcHdr))
}

// ByteArrayToString turns a slice of bytes to string, without extra memory allocations
//
// NOTE! Using this function is extremely dangerous, so it can be done with
// extra care with clear understanding how it works
func ByteArrayToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}
