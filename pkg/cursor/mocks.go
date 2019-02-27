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
package cursor

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/journal"
)

// test helpers
type testTidxService struct {
	journals map[model.TagLine]string
}

func (tis *testTidxService) GetOrCreateJournal(tags string) (string, error) {
	return "", nil
}

func (tis *testTidxService) GetJournals(exp *lql.Expression, maxSize int, checkAll bool) (map[model.TagLine]string, int, error) {
	return tis.journals, len(tis.journals), nil
}

type testJrnlCtrlr struct {
	mp map[string]*testJrnl
}

func (tjc *testJrnlCtrlr) GetOrCreate(ctx context.Context, jname string) (journal.Journal, error) {
	if j, ok := tjc.mp[jname]; ok {
		j.name = jname
		return j, nil
	}
	return nil, fmt.Errorf("Journal %s is not found", jname)
}

func (tjc *testJrnlCtrlr) GetJournals(ctx context.Context) []string {
	res := make([]string, 0, len(tjc.mp))
	for src := range tjc.mp {
		res = append(res, src)
	}
	return res
}

type testJrnl struct {
	name string
}

func (tj *testJrnl) Name() string {
	return tj.name
}

// Write - writes records received from the iterator to the journal.
// It returns number of records written, next record write position and an error if any
func (tj *testJrnl) Write(ctx context.Context, rit records.Iterator) (int, journal.Pos, error) {
	return 0, journal.Pos{}, nil
}

// Size returns the summarized chunks size
func (tj *testJrnl) Size() int64 {
	return 0
}

// Iterator returns an iterator to walk through the journal records
func (tj *testJrnl) Iterator() journal.Iterator {
	return &testJIterator{journal: tj.name}
}

// Sync could be called after a write to sync the written data with the
// storage to be sure the read will be able to read the new added
// data
func (tj *testJrnl) Sync() {

}

type testJIterator struct {
	journal string
	pos     journal.Pos
}

func (tji *testJIterator) Close() error {
	return nil
}

func (tji *testJIterator) Next(ctx context.Context) {

}

func (tji *testJIterator) Get(ctx context.Context) (records.Record, error) {
	return nil, nil
}

func (tji *testJIterator) Pos() journal.Pos {
	return tji.pos
}

func (tji *testJIterator) SetPos(pos journal.Pos) {
	tji.pos = pos
}

func (tji *testJIterator) Release() {

}
