// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this f except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"bufio"
	"context"
	"github.com/logrange/logrange/collector/utils"
	"io"
	"time"
)

type (
	lineReader struct {
		r        *bufio.Reader
		eofSleep time.Duration
	}
)

const (
	sleepOnEOF = 200
)

func newLineReader(ioRdr io.Reader, bufSize int) *lineReader {
	r := new(lineReader)
	r.r = bufio.NewReaderSize(ioRdr, bufSize)
	r.eofSleep = sleepOnEOF * time.Millisecond
	return r
}

// readLine reads lines from provided reader until EOF is met.
// It follows the io.Reader.Read contract and returns io.EOF
// only when it doesn't have data to be read.
func (r *lineReader) readLine(ctx context.Context) ([]byte, error) {
	var buf []byte
	for ctx.Err() == nil {
		line, err := r.r.ReadSlice('\n')
		line = utils.BytesCopy(line)
		if err == nil {
			return concatBufs(buf, line), err
		}

		if err == io.EOF {
			buf = concatBufs(buf, line)
			if len(buf) == 0 {
				return nil, io.EOF
			}
			utils.Sleep(ctx, r.eofSleep)
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

func concatBufs(b1, b2 []byte) []byte {
	if len(b1) == 0 {
		return b2
	}
	nb := make([]byte, len(b1)+len(b2))
	copy(nb[:len(b1)], b1)
	copy(nb[len(b1):], b2)
	return nb
}
