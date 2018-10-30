package fs

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
