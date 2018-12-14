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

package model

import (
	"context"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/kv"
	"github.com/logrange/logrange/pkg/kv/inmem"
	"github.com/logrange/logrange/pkg/records/chunk"
)

func TestGetJournalInfo(t *testing.T) {
	jc := constructJournalCatalog(t, inmem.New())

	ji, err := jc.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if ji.Journal != "test" || len(ji.Chunks) != 0 || len(ji.LocalChunks) != 0 {
		t.Fatal("must be no records")
	}

	err = jc.ReportLocalChunks(context.Background(), "test", []chunk.Id{chunk.Id(1), chunk.Id(2)})
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	ji, err = jc.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if ji.Journal != "test" || len(ji.Chunks) != 0 || len(ji.LocalChunks) != 2 {
		t.Fatal("must be no records")
	}

	err = jc.ReportLocalChunks(context.Background(), "test", []chunk.Id{chunk.Id(12)})
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	ji, err = jc.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if ji.Journal != "test" || len(ji.Chunks) != 0 || len(ji.LocalChunks) != 1 || ji.LocalChunks[0] != 12 {
		t.Fatal("must be no records")
	}
}

func TestReportEmptyJournalInfo(t *testing.T) {
	jc := constructJournalCatalog(t, inmem.New())

	err := jc.ReportLocalChunks(context.Background(), "test", nil)
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	err = jc.ReportLocalChunks(context.Background(), "test", []chunk.Id{})
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	err = jc.ReportLocalChunks(context.Background(), "test", []chunk.Id{chunk.Id(1), chunk.Id(2)})
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	jc.ReportLocalChunks(context.Background(), "test", nil)
	ji, err := jc.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if ji.Journal != "test" || len(ji.Chunks) != 0 || len(ji.LocalChunks) != 0 {
		t.Fatal("must be no records")
	}
}

func TestGetJournalInfo2Hosts(t *testing.T) {
	jc1 := constructJournalCatalog(t, inmem.New())
	jc2 := constructJournalCatalog(t, jc1.Strg)

	err := jc1.ReportLocalChunks(context.Background(), "test", []chunk.Id{chunk.Id(1), chunk.Id(2)})
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	// Cross check 1
	ji, err := jc1.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if ji.Journal != "test" || len(ji.Chunks) != 0 || len(ji.LocalChunks) != 2 {
		t.Fatal("must be no records")
	}

	ji, err = jc2.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if ji.Journal != "test" || len(ji.Chunks) != 2 || len(ji.LocalChunks) != 0 {
		t.Fatal("must be no records")
	}
	if len(ji.Chunks[1]) != 1 || ji.Chunks[1][0] != jc1.HstReg.Id() || ji.Chunks[2][0] != jc1.HstReg.Id() {
		t.Fatal("Wrong record ", ji)
	}

	err = jc2.ReportLocalChunks(context.Background(), "test", []chunk.Id{chunk.Id(12)})
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	// Cross check 2
	ji, err = jc1.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if len(ji.Chunks) != 1 || len(ji.LocalChunks) != 2 || ji.Chunks[12][0] != jc2.HstReg.Id() {
		t.Fatal("must be no records")
	}

	ji, err = jc2.GetJournalInfo(context.Background(), "test")
	if err != nil {
		t.Fatal("unexpected err=", err)
	}
	if len(ji.Chunks) != 2 || len(ji.LocalChunks) != 1 || ji.Chunks[1][0] != jc1.HstReg.Id() || ji.LocalChunks[0] != 12 {
		t.Fatal("must be no records")
	}
}

func constructJournalCatalog(t *testing.T, ms kv.Storage) *journalCatalog {
	hr := NewHostRegistry()
	hr.Cfg = &hrConfig{leaseTTL: time.Second}
	hr.Strg = ms
	err := hr.Init(context.Background())
	if err != nil {
		t.Fatal("Must be initialized ok, but got the err=", err)
	}

	jc := NewJournalCatalog()
	jc.Strg = ms
	jc.HstReg = hr
	return jc
}
