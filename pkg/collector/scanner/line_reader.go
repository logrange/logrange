package scanner

import (
	"bufio"
	"context"
	"github.com/logrange/logrange/pkg/util"
	"io"
	"time"
)

type (
	lineReader struct {
		ctx    context.Context
		cancel context.CancelFunc
		r      *bufio.Reader
		eofTO  time.Duration
	}
)

const (
	cSleepWhenEOFMs   = 200 * time.Millisecond
)

func newLineReader(ioRdr io.Reader, bufSize int, ctx context.Context) *lineReader {
	r := new(lineReader)
	r.r = bufio.NewReaderSize(ioRdr, bufSize)
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.eofTO = cSleepWhenEOFMs
	return r
}

// readLine reads lines from provided reader until EOF is met. It follows the
// io.Reader.Read contract and returns io.EOF only when it doesn't have data to be
// read.
func (r *lineReader) readLine() ([]byte, error) {
	var buf []byte
	for r.ctx.Err() == nil {
		line, err := r.r.ReadSlice('\n')
		line = util.BytesCopy(line)
		if err == nil {
			return concatBufs(buf, line), err
		}

		if err == io.EOF {
			buf = concatBufs(buf, line)
			if len(buf) == 0 {
				return nil, io.EOF
			}
			r.sleep(r.eofTO)
			continue
		}

		if err == bufio.ErrBufferFull {
			return concatBufs(buf, line), nil
		}
		return nil, err
	}
	return nil, io.ErrClosedPipe
}

func (r *lineReader) reset(ioRdr io.Reader) {
	r.r.Reset(ioRdr)
}

func (r *lineReader) sleep(to time.Duration) {
	select {
	case <-r.ctx.Done():
		return
	case <-time.After(to):
		return
	}
}

func (r *lineReader) Close() error {
	r.cancel()
	return nil
}

func concatBufs(b1, b2 []byte) []byte {
	if len(b1) == 0 {
		return b2
	}
	nb := make([]byte, len(b1)+len(b2))
	copy(nb[:len(b1)], b1)
	copy(nb[len(b1):], b2)
	return nb
}
