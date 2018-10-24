package inmem

import (
	"io"

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
