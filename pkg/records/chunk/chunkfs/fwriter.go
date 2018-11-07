// Copyright 2018 The logrange Authors
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
	"bufio"
	"io"
	"os"

	"github.com/logrange/logrange/pkg/util"
)

type (
	fWriter struct {
		fd    *os.File
		fdPos int64
		bw    *bufio.Writer
	}
)

func newFWriter(file string, bufSize int) (*fWriter, error) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}

	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &fWriter{
		fd:    f,
		fdPos: offset,
		bw:    bufio.NewWriterSize(f, bufSize),
	}, nil
}

func (w *fWriter) Close() error {
	w.bw.Flush()
	w.fd.Sync()
	err := w.fd.Close()
	w.bw = nil
	w.fd = nil
	return err
}

func (w *fWriter) size() int64 {
	return w.fdPos
}

// write - writes data into the buffer and returns position BEFORE the write
func (w *fWriter) write(data []byte) (int64, error) {
	if w.bw == nil {
		return -1, util.ErrWrongState
	}
	offset := w.fdPos
	nn, err := w.bw.Write(data)
	w.fdPos += int64(nn)
	return offset, err
}

func (w *fWriter) flush() {
	w.bw.Flush()
}

// buffered returns number of bytes in the buffer
func (w *fWriter) buffered() int {
	return w.bw.Buffered()
}
