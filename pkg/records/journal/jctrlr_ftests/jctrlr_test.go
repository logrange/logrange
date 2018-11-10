// Copyright 2018 The logrange Authors
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

package jctrl_ftests

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/logrange/logrange/pkg/records/journal/jctrlr"
)

type testCtx struct {
	cfg   jctrlr.Config
	ctrlr *jctrlr.Controller
}

func TestWriteAndScan(t *testing.T) {
	tc := prepareTestCtx(t, "FuncJournalTest")
	defer tc.Shutdown()

	tc.ctrlr.Scan(false)
}

func (tc *testCtx) Shutdown() {
	defer os.RemoveAll(tc.cfg.BaseDir)
	tc.close()
}

func (tc *testCtx) close() {
	if tc.ctrlr != nil {
		tc.ctrlr.Close()
	}
	tc.ctrlr = nil
}

func (tc *testCtx) open() {
	tc.ctrlr = jctrlr.New(tc.cfg)
}

func prepareTestCtx(t *testing.T, dirname string) *testCtx {
	dir, err := ioutil.TempDir("", dirname)
	if err != nil {
		t.Fatal("Could not create new temporary dir ", dirname, " err=", err)
	}
	t.Log(dir)
	tc := new(testCtx)
	tc.cfg.BaseDir = dir
	tc.cfg.FdPoolSize = 4 // 2 simultaneously readers only
	tc.open()
	return tc
}
