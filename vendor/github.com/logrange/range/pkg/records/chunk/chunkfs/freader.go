// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkfs

import (
	"fmt"
	"io"
	"math"
	"os"

	"sync/atomic"
)

type (
	// fReader provides buffered file read functionality, and some "smart" seeking a position
	// in the file.
	fReader struct {
		filename string
		fd       *os.File
		pos      int64 // file real offset

		// internal buffer
		buf []byte
		r   int // buf read position
		w   int // buf write position

		// pool control state, it can be set only by one goroutine at a time,
		// but it can be read by many go-routines
		plState int32
	}
)

const (
	frStateFree    = 0
	frStateBusy    = 1
	frStateClosing = 2
	frStateClosed  = 3
)

// newFReader constructs new fReader instance
func newFReader(filename string, bufSize int) (*fReader, error) {
	r := new(fReader)
	r.filename = filename
	r.buf = make([]byte, bufSize, bufSize)

	var err error
	r.fd, err = os.OpenFile(r.filename, os.O_RDONLY, 0640)
	if err != nil {
		return nil, err
	}
	r.resetBuf()
	r.plState = frStateFree

	return r, nil
}

func (r *fReader) reopen() error {
	var err error
	if r.fd != nil {
		err = r.fd.Close()
		r.resetBuf()
		r.fd = nil
		r.pos = 0
	}
	r.fd, err = os.OpenFile(r.filename, os.O_RDONLY, 0640)
	return err
}

func (r *fReader) String() string {
	return fmt.Sprintf("{fn=%s, pos=%d, bufLen=%d, r=%d, w=%d, plState=%d}", r.filename, r.pos, len(r.buf), r.r, r.w, r.plState)
}

// resetBuf drops the buffer - makes it empty.
func (r *fReader) resetBuf() {
	r.r = 0
	r.w = 0
}

// Read - implements io.Reader contract for the reader.
// The function will panic if the reader is not opened or closed.
func (r *fReader) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, nil
	}

	if r.r == r.w {
		// the buffer is empty, should we make direct read?
		if len(p) >= len(r.buf) {
			n, err := r.fd.Read(p)
			if n < 0 {
				panic("Negative read result, from file")
			}
			r.pos += int64(n)
			return n, err
		}

		// requested buffer p is less than internal buffer. Fill internal then.
		r.resetBuf()
		n, err := r.fd.Read(r.buf)
		if n < 0 {
			panic("Negative read result, from file")
		}
		if n == 0 {
			return 0, err
		}
		r.w += n
		r.pos += int64(n)
	}

	// copy as much as we can in to the result buffer
	n = copy(p, r.buf[r.r:r.w])
	r.r += n
	return n, nil
}

// seek moves the read position to the desired offset. It saves (does not make it)
// system call (Seek()) on the file if the desired offset is in within the buffer.
func (r *fReader) seek(offset int64) error {
	bOffset := r.pos - int64(r.w)
	if r.pos > offset && bOffset <= offset {
		// oh God, we have the position in the buffer!
		r.r = int(offset - bOffset)
		return nil
	}

	return r.seekPhysical(offset)
}

// seekPhysical sets the file read position to the offset. It will return
// error if the actial offset after calling the Seek is different than requested one.
// So if offset is behind the file limits, the function returns ErrWrongOffset.
func (r *fReader) seekPhysical(offset int64) error {
	off, err := r.fd.Seek(offset, io.SeekStart)
	if off != offset {
		return ErrWrongOffset
	}

	r.pos = offset
	r.resetBuf()
	return err
}

// seekToEnd sets the read position to the end of the file.  getNextReadPos()
// must return the file size immeditaly after the call
func (r *fReader) seekToEnd() error {
	off, err := r.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	r.pos = off
	r.resetBuf()
	return nil
}

// smartSeek allows to set position to the desired offset, but filling the buffer not
// for forward reading, but backward instead. bufBottom specify the size of the buffer
// that should be read after offset, but if consecuitive reads will happen with offset
// less than specified one, most probably they will hit the buffer again.
//
// For example: If the buffer size is 1000 and the desired offset is 5000,
// with bufBottom == 50, the buffer will be filled with physical offset=4050.
// The buffer will contain bytes in the range [4050..5050), which includes the
// offset=5000, the 50 extra bytes will be stored in the buffer, and further backward
// reads with offset < 5000 will also probably hit the buffer.
func (r *fReader) smartSeek(offset int64, bufBottom int) error {
	if bufBottom == 0 || bufBottom > len(r.buf) {
		return r.seek(offset)
	}

	// is the offset in the buffer and we have all the bytes already there?
	bOffset := r.pos - int64(r.w)
	if r.pos > offset && bOffset <= offset && offset+int64(bufBottom) < r.pos {
		r.r = int(offset - bOffset)
		return nil
	}

	err := r.fillBuff(offset - int64(len(r.buf)-bufBottom))
	if err != nil {
		return err
	}
	return r.seek(offset)
}

func (r *fReader) getNextReadPos() int64 {
	return r.pos - int64(r.w-r.r)
}

func (r *fReader) fillBuff(offset int64) error {
	if offset < 0 {
		offset = 0
	}
	err := r.seekPhysical(offset)
	if err != nil {
		return err
	}

	n, err := r.fd.Read(r.buf)
	if n < 0 {
		panic("Negative read result, received from the file read")
	}
	r.w += n
	r.pos += int64(n)
	return err
}

// read - reads the required buffer size even. Returns number of bytes read and
// a error, if it happens
func (r *fReader) read(b []byte) (int, error) {
	return io.ReadFull(r, b)
}

// distance gives a cost of seek operation for the reader. The smallest value is
// better. The distance considered to be better if the position should be
// advanced forward, than moving it back.
func (r *fReader) distance(pos int64) uint64 {
	if pos >= r.pos-int64(r.w) {
		if pos < r.pos {
			// within the buffer
			return 0
		}
		return uint64(pos - r.pos + 1)
	}
	return uint64(r.pos-pos) + math.MaxInt64
}

func (r *fReader) isFree() bool {
	return atomic.LoadInt32(&r.plState) == frStateFree
}

func (r *fReader) makeBusy() bool {
	return atomic.CompareAndSwapInt32(&r.plState, frStateFree, frStateBusy)
}

func (r *fReader) makeFree() bool {
	return atomic.CompareAndSwapInt32(&r.plState, frStateBusy, frStateFree)
}

func (r *fReader) Close() error {
	if atomic.CompareAndSwapInt32(&r.plState, frStateFree, frStateClosed) {
		return r.close()
	}

	atomic.CompareAndSwapInt32(&r.plState, frStateBusy, frStateClosing)
	return nil
}

func (r *fReader) close() error {
	var err error
	if r.fd != nil {
		err = r.fd.Close()
		r.resetBuf()
		r.fd = nil
		r.pos = 0
		atomic.StoreInt32(&r.plState, frStateClosed)
	}
	return err

}
