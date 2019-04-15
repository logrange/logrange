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
	K8sJsonLogParser struct {
		fn  string
		f   *os.File
		lr  *lineReader
		pos int64
	}

	// K8sJsonLogRec defines format of k8s json log line
	K8sJsonLogRec struct {
		Log    string    `json:"log"`
		Stream string    `json:"stream"`
		Time   time.Time `json:"time"`
	}
)

func NewK8sJsonParser(fileName string, maxRecSize int) (*K8sJsonLogParser, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	_, err = f.Stat()
	if err != nil {
		return nil, err
	}

	jp := new(K8sJsonLogParser)
	jp.fn = fileName
	jp.f = f
	jp.lr = newLineReader(f, maxRecSize)
	return jp, nil
}

func (jp *K8sJsonLogParser) NextRecord(ctx context.Context) (*model.Record, error) {
	line, err := jp.lr.readLine(ctx)
	if err != nil {
		return nil, err
	}

	var r K8sJsonLogRec
	err = json.Unmarshal(line, &r)
	if err != nil {
		return nil, err
	}

	rec := model.NewRecord(*(*[]byte)(unsafe.Pointer(&r.Log)), r.Time)
	rec.SetTag("stream", r.Stream)
	jp.pos += int64(len(line))
	return rec, nil
}

func (jp *K8sJsonLogParser) SetStreamPos(pos int64) error {
	if _, err := jp.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	jp.pos = pos
	jp.lr.reset(jp.f)
	return nil
}

func (jp *K8sJsonLogParser) GetStreamPos() int64 {
	return jp.pos
}

func (jp *K8sJsonLogParser) Close() error {
	return jp.f.Close()
}

func (jp *K8sJsonLogParser) GetStats() *Stats {
	size := int64(-1) // in case of error...
	fi, err := jp.f.Stat()
	if err == nil {
		size = fi.Size()
	}

	pStat := NewJsonStats()
	pStat.FileStats.Size = size
	pStat.FileStats.Pos = jp.GetStreamPos()
	return pStat
}
