// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
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
	"github.com/logrange/logrange/pkg/collector/model"
	"github.com/logrange/logrange/pkg/collector/scanner/parser/date"
	"io"
)

type (
	// Parser provides an interface for retrieving records from a data-stream.
	// Implementations of the interface are supposed to be initialized by the
	// stream (io.Reader)
	Parser interface {
		io.Closer

		// NextRecord parses next record. It returns error if it could not parse
		// a record from the stream. io.EOF is returned if no new records found, but
		// end is reached.
		NextRecord() (*model.Record, error)

		// SetStreamPos specifies the stream position for the next record read
		SetStreamPos(pos int64) error

		// GetStreamPos returns position of the last successfully (error was nil)
		// returned record by nextRecord(). If nextRecord() returned non-nil
		// error the getStreamPos() returned value is not relevant and should
		// not be used as a valid stream position.
		GetStreamPos() int64

		// GetStat returns the parser statistic
		GetStats() *Stats
	}

	fmtStats struct {
		hits map[string]int64
	}

	fileStats struct {
		Size int64
		Pos  int64
	}

	DataType string

	// Stats struct contains information about the parser statistics
	Stats struct {
		DataType  DataType
		FileStats *fileStats
		FmtStats  *fmtStats // for text parsers only...
	}
)

const (
	Text DataType = "text"
	Json DataType = "json"
)

const (
	UnknownFmtName = "_%_unknown_%_"
)

//===================== Stats =====================

func NewTxtStats() *Stats {
	return newStats(Text, &fileStats{})
}

func NewJsonStats() *Stats {
	return newStats(Json, &fileStats{})
}

func newStats(dataType DataType, fileStats *fileStats) *Stats {
	pStats := new(Stats)
	pStats.DataType = dataType
	pStats.FileStats = fileStats
	pStats.FmtStats = &fmtStats{make(map[string]int64)}
	return pStats
}

func (s *Stats) Update(dFmt *date.Format) {
	name := UnknownFmtName
	if dFmt != nil {
		name = dFmt.GetFormat()
	}
	s.FmtStats.hits[name]++
}

//===================== fmtStats =====================

func (fs *fmtStats) Count() (int64, int64, int64) {
	total := int64(0)
	for _, v := range fs.hits {
		total += v
	}
	failed := fs.hits[UnknownFmtName]
	return total, total - failed, failed
}

func (fs *fmtStats) Copy() *fmtStats {
	return &fmtStats{fs.Hits()}
}

func (fs *fmtStats) Hits() map[string]int64 {
	hitsCopy := make(map[string]int64)
	for k, v := range fs.hits {
		hitsCopy[k] = v
	}
	return hitsCopy
}
