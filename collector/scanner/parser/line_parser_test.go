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
	"bytes"
	"context"
	"github.com/logrange/logrange/collector/model"
	"github.com/logrange/logrange/collector/scanner/parser/date"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseAndReposition(t *testing.T) {
	fn := absPath("../testdata/logs/ubuntu/var/log/mongodb/mongodb.log")
	lp, err := NewLineParser(fn, date.NewDefaultParser(), 4096)
	assert.NoError(t, err)

	pos := make([]int64, 0)
	rec := make([]*model.Record, 0)
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			pos = append(pos, lp.GetStreamPos())
			r, err := lp.NextRecord(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, r)
			rec = append(rec, r)
		}
	}

	for i := 0; i < len(pos); i++ { // shuffle a bit
		rand.Seed(time.Now().UnixNano())
		ri := rand.Intn(len(pos))
		pos[i], rec[i] = pos[ri], rec[ri]
	}

	for i, exp := range rec {
		err = lp.SetStreamPos(pos[i])
		assert.NoError(t, err)
		act, err := lp.NextRecord(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, act)
		assert.Equal(t, exp, act)
	}

	err = lp.Close()
	assert.NoError(t, err)
}

func TestParseKnownFormats(t *testing.T) {
	files := []string{
		absPath("../testdata/logs/ubuntu/var/log/mongodb/mongodb.log"),
		absPath("../testdata/logs/ubuntu/var/log/nginx/access.log"),
		absPath("../testdata/logs/ubuntu/var/log/postgresql/postgresql-9.5-main.log"),
		absPath("../testdata/logs/ubuntu/var/log/dpkg.log"),
		absPath("../testdata/logs/ubuntu/var/log/alternatives.log"),
		absPath("../testdata/logs/ubuntu/var/log/kern.log"),
	}

	for _, fn := range files {
		lp, err := NewLineParser(fn, date.NewDefaultParser(), 4096)
		assert.NoError(t, err)
		for err == nil {
			_, err = lp.NextRecord(context.Background())
			assert.Equal(t, parsing, lp.state)
		}

		assert.EqualError(t, err, io.EOF.Error())
		count, err := lineCount(fn)
		assert.NoError(t, err)

		total, success, failed := lp.GetStats().FmtStats.Count()
		assert.Equal(t, int64(count), total)
		assert.Equal(t, int64(count), success)
		assert.Equal(t, int64(0), failed)
		assert.Equal(t, lp.GetStats().FileStats.Size, lp.GetStats().FileStats.Pos)

		err = lp.Close()
		assert.NoError(t, err)
	}
}

func TestParseWithSkipping(t *testing.T) {
	fn := absPath("../testdata/logs/ubuntu/var/log/apt/term.log")
	lp, err := NewLineParser(fn, date.NewDefaultParser(), 4096)
	assert.NoError(t, err)

	defer func() {
		err = lp.Close()
		assert.NoError(t, err)
	}()

	validate := func() {
		r, err := lp.NextRecord(context.Background())
		assert.NoError(t, err)
		act := r.GetDate()
		assert.Equal(t, 2017, act.Year())
		assert.Equal(t, time.January, act.Month())
		assert.Equal(t, 23, act.Day())
	}

	for lp.state != skipping {
		validate()
	}

	for lp.state != parsing {
		validate()
	}

	for lp.state != skipping {
		validate()
	}
}

func absPath(relative string) string {
	absolute, err := filepath.Abs(relative)
	if err != nil {
		panic(err)
	}
	return absolute
}

func lineCount(fn string) (int, error) {
	buf := make([]byte, 32*1024)
	cnt, sep := 0, []byte{'\n'}

	f, err := os.Open(fn)
	if err != nil {
		return -1, err
	}

	for {
		c, err := f.Read(buf)
		cnt += bytes.Count(buf[:c], sep)

		switch {
		case err == io.EOF:
			return cnt, nil

		case err != nil:
			return cnt, err
		}
	}
}
