package scanner

import (
	"context"
	"encoding/json"
	"github.com/logrange/logrange/pkg/collector/model"
	"io"
	"os"
	"time"
	"unsafe"
)

type (
	// jsonParser implements Parser, for reading lines(strings
	// with /n separation) from a text file, treating every of the lines
	// as a json message.
	//
	//This parser works ok with standard k8s logs output as
	// a line per record, but in long-term prospective it needs to be reworked
	// to address the restrictions it has now.
	jsonParser struct {
		fname    string
		fReader  *os.File
		lnReader *lineReader
		pos      int64
	}

	// jsonRec defines format of every line, which has the object JSON enconding.
	jsonRec struct {
		Log    string    `json:"log"`
		Stream string    `json:"stream"`
		Time   time.Time `json:"time"`
	}
)

func newJsonParser(fileName string, maxRecSize int, ctx context.Context) (*jsonParser, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	jp := new(jsonParser)
	jp.fname = fileName
	jp.fReader = f
	jp.lnReader = newLineReader(f, maxRecSize, ctx)
	return jp, nil
}

func (jp *jsonParser) NextRecord() (*model.Record, error) {
	line, err := jp.lnReader.readLine()
	if err != nil {
		return nil, err
	}

	var r jsonRec
	err = json.Unmarshal(line, &r)
	if err != nil {
		return nil, err
	}

	rec := model.NewEmptyRecord()
	rec.Data = *(*[]byte)(unsafe.Pointer(&r.Log))
	rec.Meta["stream"] = r.Stream
	rec.SetTs(r.Time)

	jp.pos += int64(len(line))
	return rec, nil
}

func (jp *jsonParser) SetStreamPos(pos int64) error {
	if _, err := jp.fReader.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	jp.pos = pos
	jp.lnReader.reset(jp.fReader)
	return nil
}

func (jp *jsonParser) GetStreamPos() int64 {
	return jp.pos
}

func (jp *jsonParser) Close() error {
	jp.lnReader.Close()
	return jp.fReader.Close()
}

func (jp *jsonParser) GetStats() *ParserStats {
	ps := &ParserStats{}
	ps.DataType = "JSON"
	ps.Pos = jp.GetStreamPos()
	fi, _ := jp.fReader.Stat()
	if fi != nil {
		ps.Size = fi.Size()
	}
	return ps
}
