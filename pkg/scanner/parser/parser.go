// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a MakeCopy of the License at
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
	"fmt"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/logrange/logrange/pkg/scanner/parser/date"
	"io"
	"strings"
)

type (
	// Parser provides an interface for retrieving records from a data-pipe.
	// Implementations of the interface are supposed to be initialized by the
	// pipe (io.Reader)
	Parser interface {
		io.Closer

		// NextRecord parses next record. It returns error if it could not parse
		// a record from the pipe. io.EOF is returned if no new records found, but
		// end is reached.
		NextRecord(ctx context.Context) (*model.Record, error)

		// SetStreamPos specifies the pipe position for the next record read
		SetStreamPos(pos int64) error

		// GetStreamPos returns position of the last successfully (error was nil)
		// returned record by nextRecord(). If nextRecord() returned non-nil
		// error the getStreamPos() returned value is not relevant and should
		// not be used as a valid pipe position.
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

	DataFormat string

	// Stats struct contains information about the parser statistics
	Stats struct {
		DataFormat DataFormat
		FileStats  *fileStats
		FmtStats   *fmtStats // for text parsers only...
	}
)

const (
	FmtPure   DataFormat = "pure"
	FmtText   DataFormat = "text"
	FmtK8Json DataFormat = "k8json"
	FmtLogfmt DataFormat = "logfmt"
)

const (
	unknownDateFmtName = "_%_unknown_%_"
)

func ToDataFormat(str string) (DataFormat, error) {
	str = strings.ToLower(strings.Trim(str, " "))
	df := DataFormat(str)
	var err error
	switch df {
	case FmtPure:
	case FmtText:
	case FmtK8Json:
	case FmtLogfmt:
	default:
		err = fmt.Errorf("unknown date format \"%s\", expecting \"%s\", \"%s\" or \"%s\" or \"%s\"", str, FmtPure, FmtText, FmtK8Json,FmtLogfmt)
	}
	return df, err
}

// NewParser creates new parser based on the type the parser specified in cfg.DataFmt.
func NewParser(cfg *Config) (Parser, error) {
	if err := cfg.Check(); err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}

	var (
		p   Parser
		err error
	)

	switch cfg.DataFmt {
	case FmtPure:
		p, err = NewPureParser(cfg.File, cfg.MaxRecSizeBytes)
	case FmtText:
		p, err = NewLineParser(cfg.File,
			date.NewDefaultParser(cfg.DateFmts...), cfg.MaxRecSizeBytes)
	case FmtK8Json:
		p, err = NewK8sJsonParser(cfg.File, cfg.MaxRecSizeBytes)
	case FmtLogfmt:
		p, err = NewLogfmtParser(cfg.File, cfg.MaxRecSizeBytes,cfg.FieldMap)
	default:
		err = fmt.Errorf("unknown parser for data format=%s", cfg.DataFmt)
	}

	return p, err
}

func newStats(dataFormat DataFormat, fileStats *fileStats) *Stats {
	pStats := new(Stats)
	pStats.DataFormat = dataFormat
	pStats.FileStats = fileStats
	pStats.FmtStats = &fmtStats{make(map[string]int64)}
	return pStats
}

// Update adds 1 hit to the date format statistic
func (s *Stats) Update(dFmt *date.Format) {
	name := unknownDateFmtName
	if dFmt != nil {
		name = dFmt.GetFormat()
	}
	s.FmtStats.hits[name]++
}

// Count returns the number of total, successful and failed all formats hits
func (fs *fmtStats) Count() (int64, int64, int64) {
	total := int64(0)
	for _, v := range fs.hits {
		total += v
	}
	failed := fs.hits[unknownDateFmtName]
	return total, total - failed, failed
}

// Copy makes a copy of the object
func (fs *fmtStats) Copy() *fmtStats {
	return &fmtStats{fs.Hits()}
}

// Hits returns the statistic by format hits
func (fs *fmtStats) Hits() map[string]int64 {
	hitsCopy := make(map[string]int64)
	for k, v := range fs.hits {
		hitsCopy[k] = v
	}
	return hitsCopy
}
