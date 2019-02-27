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

package scanner

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/collector/model"
	"github.com/logrange/logrange/collector/scanner/parser"
	"github.com/logrange/logrange/collector/storage"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const (
	testOutDir = "/tmp/scannertest"
)

var (
	inOutMap = map[string]string{}
	outFiles = map[string]*os.File{}
	log      = log4g.GetLogger("TestIntegration")
)

func setUp(t *testing.T) {
	err := os.RemoveAll(testOutDir)
	assert.NoError(t, err)

	err = os.MkdirAll(testOutDir, 0777)
	assert.NoError(t, err)
}

func tearDown() {
	_ = os.RemoveAll(testOutDir)
}

func TestIntegration(t *testing.T) {
	setUp(t)
	defer tearDown()

	cl, err := NewScanner(config(), storage.NewDefaultStorage())
	if err != nil {
		t.Fatal(err)
	}

	events := make(chan *model.Event)
	ctx, cancel := context.WithCancel(context.Background())

	if err := cl.Run(events, ctx); err != nil {
		t.Fatal(err)
	}

	for {
		select {
		case ev := <-events:
			if err := handle(ev); err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			cancel()
			_ = cl.Close()

			close(events)
			validate(t)
			return
		}
	}
}

func config() *Config {
	log4g.SetLogLevel("", log4g.INFO)

	cfg := NewDefaultConfig()
	cfg.ScanPathsIntervalSec = 5

	_, d, _, _ := runtime.Caller(0)
	cfg.IncludePaths = []string{
		filepath.Dir(d) + "/testdata/logs/ubuntu/var/log/*.log",
		filepath.Dir(d) + "/testdata/logs/ubuntu/var/log/*/*.log",
	}

	cfg.ExcludeMatchers = []string{
		".*auth.log.*",
	}

	cfg.Schemas = []*SchemaConfig{
		{
			PathMatcher: "/*(?:.+/)*(?P<file>.+\\..+)",
			DataFormat:  parser.FmtText,
			DateFormats: []string{"DD/MMM/YYYY:HH:mm:ss ZZZZ"},
			Meta: Meta{
				Tags: map[string]string{
					"file": "test-{file}",
					"note": "abc",
				},
			},
		},
	}
	return cfg
}

func validate(t *testing.T) {
	closeOutFiles()
	if len(inOutMap) == 0 {
		log.Error("nothing was sent, nothing to compare!")
		t.FailNow()
	}
	for src, dst := range inOutMap {
		log.Debug("comparing src=", src, ", dst=", dst)
		eq, err := md5Eq(src, dst)
		if err != nil || !eq {
			log.Error("failed to compare src=", src, ", dst=", dst, "; err=", err, ", eq=", eq)
			t.Fail()
		}
	}
}

func handle(ev *model.Event) error {
	tFile := fmt.Sprintf("%v/%v", testOutDir, path.Base(ev.File))
	for _, r := range ev.Records {
		if err := appendToFile(tFile, r); err != nil {
			return err
		}
	}

	ev.Confirm()
	inOutMap[ev.File] = tFile
	return nil
}

func appendToFile(file string, r *model.Record) error {
	fd, err := getOutFile(file)
	if err != nil {
		return err
	}

	/*	bb := bytes.Buffer{}
		bb.Write([]byte{'\n', '>', 't', 's', '='})
		bb.Write([]byte((*r.GetDate()).Format(time.RFC3339Nano)))
		bb.Write([]byte{'|'})
		bb.Write(r.Data)
		bb.Write([]byte{'<'})*/
	_, err = fd.Write(r.Data)
	return err
}

func getOutFile(file string) (*os.File, error) {
	if f, ok := outFiles[file]; ok {
		return f, nil
	}

	fd, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}

	outFiles[file] = fd
	return fd, nil
}

func closeOutFiles() {
	for _, fd := range outFiles {
		_ = fd.Sync()
		_ = fd.Close()
	}
}

func md5Eq(f1, f2 string) (bool, error) {
	f1h, err := md5File(f1)
	log.Debug("file ", f1, " hash=", f1h)
	if err != nil {
		return false, err
	}

	f2h, err := md5File(f2)
	log.Debug("file ", f2, " hash=", f2h)
	if err != nil {
		return false, err
	}

	return bytes.Equal(f1h, f2h), nil
}

func md5File(file string) ([]byte, error) {
	fi, _ := os.Stat(file)
	log.Debug("file=", file, " size=", fi.Size())

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = f.Close()
	}()

	hash := md5.New()
	if _, err := io.Copy(hash, f); err != nil {
		return nil, err
	}

	var result []byte
	return hash.Sum(result), err
}
