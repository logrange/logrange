package scanner

import (
	"context"
	"github.com/logrange/logrange/pkg/collector/model"
	"io"
	"os"
)

type (
	// lines_parser implements parser, for reading from a file lines(strings
	// with /n separation) one by one and transforms them to records
	linesParser struct {
		fname    string
		fReader  *os.File
		lnReader *lineReader
		lnParser *stringParser
		pos      int64
	}
)

func newLinesParser(fileName string, dtFormats []string, maxRecSize int, ctx context.Context) (*linesParser, error) {
	lnParser := newStringParser(dtFormats...)

	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	lp := new(linesParser)
	lp.fname = fileName
	lp.fReader = f
	lp.lnReader = newLineReader(f, maxRecSize, ctx)
	lp.lnParser = lnParser
	return lp, nil
}

func (lp *linesParser) NextRecord() (*model.Record, error) {
	line, err := lp.lnReader.readLine()
	if err != nil {
		return nil, err
	}
	lp.pos += int64(len(line))
	return lp.lnParser.apply(line), nil
}

func (lp *linesParser) SetStreamPos(pos int64) error {
	if _, err := lp.fReader.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	lp.pos = pos
	lp.lnReader.reset(lp.fReader)
	return nil
}

func (lp *linesParser) GetStreamPos() int64 {
	return lp.pos
}

func (lp *linesParser) Close() error {
	lp.lnReader.Close()
	return lp.fReader.Close()
}

func (lp *linesParser) GetStats() *ParserStats {
	ps := &ParserStats{}
	ps.DataType = "TEXT"
	ps.DateFormats = make(map[string]int64)
	ps.Size = -1 // just in case of an error
	for k, v := range lp.lnParser.stats.hits {
		ps.DateFormats[k] = v
	}
	ps.Pos = lp.GetStreamPos()
	fi, err := os.Stat(lp.fname)
	if err == nil {
		ps.Size = fi.Size()
	}
	return ps
}
