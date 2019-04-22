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

package bytes

// Writer struct support io.Writer interface and allows to write data into extendable
// underlying buffer. It uses Pool for arranging new buffers.
//
// The Writer MUST not be copied.
type Writer struct {
	pool *Pool
	buf  []byte
	pos  int
}

// Init initilizes the Writer to use Pool p and initialize the w.buf to specified size
func (w *Writer) Init(initSz int, p *Pool) {
	w.pool = p
	w.pos = 0
	if p != nil {
		w.buf = w.pool.Arrange(initSz)
	} else {
		w.buf = make([]byte, initSz)
	}
}

// Reset drops the pos to 0, but it doesn't touch underlying buffer
func (w *Writer) Reset() {
	w.pos = 0
}

// Write is part of io.Writer
func (w *Writer) Write(p []byte) (n int, err error) {
	w.grow(len(p))
	n = copy(w.buf[w.pos:], p)
	w.pos += n
	return n, nil
}

// WriteByte writes one byte to the buffer
func (w *Writer) WriteByte(b byte) {
	w.grow(1)
	w.buf[w.pos] = b
	w.pos++
}

// WriteString writes string to the buffer
func (w *Writer) WriteString(s string) {
	w.grow(len(s))
	n := copy(w.buf[w.pos:], StringToByteArray(s))
	w.pos += n
}

// Buf returns underlying written buffer
func (w *Writer) Buf() []byte {
	return w.buf[:w.pos]
}

// Close releases resources and makes w unusable
func (w *Writer) Close() error {
	if w.pool != nil {
		w.pool.Release(w.buf)
	}
	w.buf = nil
	w.pool = nil
	return nil
}

func (w *Writer) grow(n int) {
	av := cap(w.buf) - w.pos
	if av < n {
		nbs := int(2*cap(w.buf) + n)
		if cap(w.buf) == 0 {
			nbs = 60 + 2*n
		}
		var nb []byte
		if w.pool != nil {
			nb = w.pool.Arrange(nbs)
		} else {
			nb = make([]byte, nbs)
		}
		copy(nb, w.buf[:w.pos])
		w.pool.Release(w.buf)
		w.buf = nb
	}
}
