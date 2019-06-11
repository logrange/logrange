// Copyright 2018-2019 The logrange Authors
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
	"context"
	"github.com/logrange/logrange/pkg/scanner/model"
	"io"
	"os"
	"time"
)

// pureParser struct implements Parser interface. It is probably simplest implementation
// of the Parser interface. The pureParser turns every line of text from the line reader
// to the record, timestamped by the time when the line is read.
type pureParser struct {
	Parser

	fn  string
	f   *os.File
	lr  *lineReader
	pos int64
}

// NewPureParser creates new instance of pureParser
func NewPureParser(fileName string, maxRecSize int) (*pureParser, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	_, err = f.Stat()
	if err != nil {
		return nil, err
	}

	pp := new(pureParser)
	pp.fn = fileName
	pp.f = f
	pp.lr = newLineReader(f, maxRecSize)
	return pp, nil
}

// NextRecord parses next record. It returns error if it could not parse
// a record from the pipe. io.EOF is returned if no new records found, but
// end is reached.
func (pp *pureParser) NextRecord(ctx context.Context) (*model.Record, error) {
	line, err := pp.lr.readLine(ctx)
	if err != nil {
		return nil, err
	}

	pp.pos += int64(len(line))
	return model.NewRecord(line, time.Now()), nil
}

// SetStreamPos specifies the pipe position for the next record read
func (pp *pureParser) SetStreamPos(pos int64) error {
	if _, err := pp.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	pp.pos = pos
	pp.lr.reset(pp.f)
	return nil
}

// GetStreamPos returns position of the last successfully (error was nil)
// returned record by nextRecord(). If nextRecord() returned non-nil
// error the getStreamPos() returned value is not relevant and should
// not be used as a valid pipe position.
func (pp *pureParser) GetStreamPos() int64 {
	return pp.pos
}

// GetStat returns the parser statistic
func (pp *pureParser) GetStats() *Stats {
	size := int64(-1) // in case of an error...
	fi, err := pp.f.Stat()
	if err == nil {
		size = fi.Size()
	}

	pStat := newStats(FmtPure, &fileStats{})
	pStat.FileStats.Size = size
	pStat.FileStats.Pos = pp.GetStreamPos()
	return pStat
}

// WaitAllJobsDone is part of io.Closer interface
func (pp *pureParser) Close() error {
	return pp.f.Close()
}
