// Copyright 2018-2019 The logrange Authors
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

package parser

import (
	"context"
	"encoding/json"
	"github.com/kr/logfmt"
	"github.com/logrange/logrange/pkg/scanner/model"
	"io"
	"os"
	"time"
	"unsafe"
)

type (
	// K8sJsonLogParser implements parser.Parser, for reading lines(strings with \n separation)
	// from a text file, treating every line as a k8s json log message (fixed fields).
	// The parser doesn't pay much attention to parsing dates, as they are presented as time.Time
	// object in case of k8s log and in such a case JSON deserialization should take care of it.
	// Additionally to message and date the parser also saves some additional metadata like 'pipe' (stdout/strerr),
	// which is provided in k8s logs, the result of parsing is model.Record.
	LogfmtParser struct {
		fn  string
		f   *os.File
		lr  *lineReader
		pos int64
		fields []string
	}

	// K8sJsonLogRec defines format of k8s json log line
	LogfmtJsonLogRec struct {
		Log    string    `json:"log"`
		Stream string    `json:"stream"`
		Time   time.Time `json:"time"`
	}

	Msg map[string]string

)

func NewLogfmtParser(fileName string, maxRecSize int, fieldMap map[string]string) (*LogfmtParser, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	_, err = f.Stat()
	if err != nil {
		return nil, err
	}

	lp := new(LogfmtParser)
	lp.fn = fileName
	lp.f = f
	lp.lr = newLineReader(f, maxRecSize)
	lp.fields = make([]string,0,len(fieldMap))
	for key,_ := range fieldMap{
		lp.fields = append(lp.fields,key)
		delete(fieldMap,key)
	}

	return lp, nil
}

func (m Msg) HandleLogfmt(key, val []byte) error {
	m[string(key)] = string(val);
	return nil
}

func (lp *LogfmtParser) NextRecord(ctx context.Context) (*model.Record, error) {
	line, err := lp.lr.readLine(ctx)
	if err != nil {
		return nil, err
	}

	var r LogfmtJsonLogRec
	err = json.Unmarshal(line, &r)

	if err != nil {
		return nil, err
	}

	rec := model.NewRecord(*(*[]byte)(unsafe.Pointer(&r.Log)), r.Time)
	rec.Fields = "stream=" + r.Stream

	msg := make(Msg)
	if err := logfmt.Unmarshal(rec.Data,&msg); err == nil{
		for _,key := range lp.fields{
			if val, ok := msg[key]; ok{
				if key == "time"{ //convert msg[key] to time.Time and assign time to r.Time f we could parse it
					if t,err := time.Parse(time.RFC3339Nano,msg[key]); err == nil{
						rec.Date = t
					}
				}
				rec.Fields += ","+ key +"=" + val
			}
		}
	}

	lp.pos += int64(len(line))
	return rec, nil
}


func (lp *LogfmtParser) SetStreamPos(pos int64) error {
	if _, err := lp.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	lp.pos = pos
	lp.lr.reset(lp.f)
	return nil
}

func (lp *LogfmtParser) GetStreamPos() int64 {
	return lp.pos
}

func (lp *LogfmtParser) Close() error {
	return lp.f.Close()
}

func (lp *LogfmtParser) GetStats() *Stats {
	size := int64(-1) // in case of error...
	fi, err := lp.f.Stat()
	if err == nil {
		size = fi.Size()
	}

	pStat := newStats(FmtLogfmt, &fileStats{})
	pStat.FileStats.Size = size
	pStat.FileStats.Pos = lp.GetStreamPos()
	return pStat
}

