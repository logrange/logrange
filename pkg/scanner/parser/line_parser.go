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
	"github.com/logrange/logrange/pkg/scanner/parser/date"
	"io"
	"os"
	"time"
)

type (
	// lineParser implements parser.Parser for reading from a file lines (strings
	// with \n separation) one by one then parsing dates out of those lines and transform
	// all this data into model.Record.
	//
	// lineParser for parsing dates relies on date.Parser which is passed during creation.
	// Using date.Parser, lineParser determines best date format found in current context
	// (for further dates parsing) so that not to request date.Parser to  determine date format
	// for every line, which can cost a lot of CPU cycles...
	//
	// In case if lineParser starts to fail in parsing dates, after certain amount of failures
	// it will start skipping date parsing and will start using last seen date or now() for the
	// next N lines. Later on it will resume attempts to parse the dates and so on...
	//
	// lineParser can be present in 'parsing' or 'skipping' states:
	//
	// 	- state 'parsing' means that lineParser tries to parse dates using saved format or
	//	  by recalculating date format. If format is not found last seen date or now() is returned.
	//
	// 	- state 'skipping' happens, when we reach date parsing errors threshold,
	// 	  we intentionally start skipping date parsing in order not to use too much CPU.
	// 	  Last seen date or now() is returned in such a case. As soon as we skip certain amount
	// 	  of presumably 'unparseable' lines we'll get back to 'parsing' state
	// 	  to try date parsing again and so on...
	lineParser struct {
		Parser

		f   *os.File
		lr  *lineReader
		dp  date.Parser
		pos int64

		curFmt   *date.Format // currently used date format
		stats    *Stats       // parser statistics
		lastDate time.Time    // last seen/parsed date

		state       state // current parser state
		maxFailCnt  int   // lines to fail date parsing before going to 'skipping' state
		maxSkipCnt  int   // lines to skip date parsing before going to 'parsing' state
		failSkipCnt int   // depending on the state is used to count skipped/failed line parsings
	}

	state int
)

const (
	// parsing, if fails 'maxFailCnt' times goes to 'skipping'
	parsing state = iota

	// skipping, i.e. skip parsing 'maxSkipCnt' number of lines,
	// before again going to 'parsing', last seen date (or time.Now()) is used in such a case.
	// This is done to avoid occupying too much CPU in case if format can't be found easily.
	skipping
)

func NewLineParser(fileName string, dateParser date.Parser, maxRecSize int) (*lineParser, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	_, err = f.Stat()
	if err != nil {
		return nil, err
	}

	lp := new(lineParser)
	lp.f = f
	lp.lr = newLineReader(f, maxRecSize)
	lp.stats = newStats(FmtText, &fileStats{})
	lp.state = parsing

	lp.dp = dateParser
	lp.lastDate = time.Time{}

	lp.maxFailCnt = 10
	lp.maxSkipCnt = 10
	lp.failSkipCnt = 0
	return lp, nil
}

func (lp *lineParser) NextRecord(ctx context.Context) (*model.Record, error) {
	line, err := lp.lr.readLine(ctx)
	if err != nil {
		return nil, err
	}
	lp.pos += int64(len(line))
	return lp.parse(line), nil
}

func (lp *lineParser) parse(buf []byte) *model.Record {
	var (
		ft *date.Format
		tm time.Time
	)

	ft = lp.curFmt
	if ft != nil { // use previously found date parser
		tm, err := ft.Parse(buf)
		if err == nil {
			lp.stats.Update(ft)
			return model.NewRecord(buf, tm)
		}
	}

	switch lp.state {
	case parsing:
		tm, ft = lp.dp.Parse(buf)
		if ft != nil { // found matching date parser
			lp.curFmt = ft
			lp.lastDate = tm
			lp.maxSkipCnt = 10
			lp.failSkipCnt = 0
		} else { // no date parser found
			lp.curFmt = nil
			lp.failSkipCnt++
			if lp.failSkipCnt >= lp.maxFailCnt { //reached maxFailCnt, go to 'skipping'
				lp.state = skipping
				lp.failSkipCnt = 0
			}
		}
	case skipping:
		lp.failSkipCnt++
		if lp.failSkipCnt >= lp.maxSkipCnt { // reached maxSkipCnt, go to 'parsing'
			lp.state = parsing
			lp.failSkipCnt = 0
			if lp.maxSkipCnt < 100 {
				lp.maxSkipCnt <<= 1
			}
		}
	}

	lp.stats.Update(ft)
	return model.NewRecord(buf, lp.calcDate(tm))
}

func (lp *lineParser) calcDate(tm time.Time) time.Time {
	if tm.IsZero() {
		tm = lp.lastDate
		//if tm.IsZero() {
		//	tm = time.Now()
		//}
	}
	return tm
}

func (lp *lineParser) SetStreamPos(pos int64) error {
	if _, err := lp.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	lp.pos = pos
	lp.lr.reset(lp.f)
	return nil
}

func (lp *lineParser) GetStreamPos() int64 {
	return lp.pos
}

func (lp *lineParser) Close() error {
	return lp.f.Close()
}

func (lp *lineParser) GetStats() *Stats {
	size := int64(-1) // in case of error...
	fi, err := lp.f.Stat()
	if err == nil {
		size = fi.Size()
	}

	pStat := newStats(FmtText, &fileStats{})
	pStat.FileStats.Size = size
	pStat.FileStats.Pos = lp.GetStreamPos()
	pStat.FmtStats = lp.stats.FmtStats.Copy()
	return pStat
}
