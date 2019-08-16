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
	//LogfmtParser is an extension of the K8sJsonLogParser. It treats the msg field of each record as a string in logfmt
	//form (https://brandur.org/logfmt). It parses out the fields in the msg and put them as additional fields in the
	//record. If a valid time is parsed out from the msg field, then it will replace the time in the k8s log. The result
	// parsing is model.Record.

	logfmtParser struct {
		fn     string
		f      *os.File
		lr     *lineReader
		pos    int64
		fields []string
	}

	// K8sJsonLogRec defines format of k8s json log line
	LogfmtJsonLogRec struct {
		Log    string    `json:"log"`
		Stream string    `json:"stream"`
		Time   time.Time `json:"time"`
	}

	ParsedMsg map[string]string
)

func NewLogfmtParser(fileName string, maxRecSize int, fieldMap map[string]string) (*logfmtParser, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	_, err = f.Stat()
	if err != nil {
		return nil, err
	}

	lp := new(logfmtParser)
	lp.fn = fileName
	lp.f = f
	lp.lr = newLineReader(f, maxRecSize)
	lp.fields = make([]string, 0, len(fieldMap))
	for key, _ := range fieldMap {
		lp.fields = append(lp.fields, key)
		delete(fieldMap, key)
	}

	return lp, nil
}

func (m ParsedMsg) HandleLogfmt(key, val []byte) error {
	m[string(key)] = string(val)
	return nil
}

func (lp *logfmtParser) NextRecord(ctx context.Context) (*model.Record, error) {
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

	parsedMsg := make(ParsedMsg)
	if err := logfmt.Unmarshal(rec.Data, &parsedMsg); err == nil {
		for _, key := range lp.fields {
			if val, ok := parsedMsg[key]; ok {
				if key == "time" { //convert msg[key] to time.Time and assign time to r.Time f we could parse it
					if t, err := time.Parse(time.RFC3339Nano, parsedMsg[key]); err == nil {
						rec.Date = t
					}
				}
				rec.Fields += "," + key + "=" + val
			}
		}
	}

	lp.pos += int64(len(line))
	return rec, nil
}

func (lp *logfmtParser) SetStreamPos(pos int64) error {
	if _, err := lp.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	lp.pos = pos
	lp.lr.reset(lp.f)
	return nil
}

func (lp *logfmtParser) GetStreamPos() int64 {
	return lp.pos
}

func (lp *logfmtParser) Close() error {
	return lp.f.Close()
}

func (lp *logfmtParser) GetStats() *Stats {
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
