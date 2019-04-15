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
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/journal"
)

// test helpers
//type testTidxService struct {
//	journals map[tag.Line]string
//}
//
//func (tis *testTidxService) GetOrCreateJournal(tags string) (string, error) {
//	return "", nil
//}
//
//func (tis *testTidxService) Visit(srcCond *lql.From, v tindex.VisitorF) error {
//	for tl, jrnl := range tis.journals {
//		s, err := tag.ParseSelect(tl.String())
//		if err != nil {
//			return err
//		}
//		if !v(s, jrnl) {
//			return nil
//		}
//	}
//	return nil
//}
//
//type testJrnlCtrlr struct {
//	mp map[string]*testJrnl
//}
//
//func (tjc *testJrnlCtrlr) GetOrCreate(ctx context.Context, jname string) (partition.Journal, error) {
//	if j, ok := tjc.mp[jname]; ok {
//		j.name = jname
//		return j, nil
//	}
//	return nil, fmt.Errorf("Journal %s is not found", jname)
//}
//
//func (tjc *testJrnlCtrlr) GetJournals(ctx context.Context) []string {
//	res := make([]string, 0, len(tjc.mp))
//	for src := range tjc.mp {
//		res = append(res, src)
//	}
//	return res
//}
//
//func (tjc *testJrnlCtrlr) Visit(ctx context.Context, cv partition.ControllerVisitorF) {
//	for src := range tjc.mp {
//		if !cv(&testJrnl{src}) {
//			return
//		}
//	}
//}

type testJrnlsProvider struct {
	j        map[tag.Line]*testJrnl
	released map[string]string
}

func (tjp *testJrnlsProvider) GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]journal.Journal, error) {
	res := make(map[tag.Line]journal.Journal)
	for k, v := range tjp.j {
		res[k] = v
	}
	return res, nil
}

func (tjp *testJrnlsProvider) Release(jn string) {
	if tjp.released == nil {
		tjp.released = make(map[string]string)
	}
	tjp.released[jn] = jn
}

type testJrnl struct {
	name string
}

func (tj *testJrnl) Name() string {
	return tj.name
}

// Write - writes records received from the iterator to the partition.
// It returns number of records written, next record write position and an error if any
func (tj *testJrnl) Write(ctx context.Context, rit records.Iterator) (int, journal.Pos, error) {
	return 0, journal.Pos{}, nil
}

// Size returns the summarized chunks size
func (tj *testJrnl) Size() uint64 {
	return 0
}

func (tj *testJrnl) Count() uint64 {
	return 0
}

// Iterator returns an iterator to walk through the partition records
func (tj *testJrnl) Iterator() journal.Iterator {
	return &testJIterator{journal: tj.name}
}

func (tj *testJrnl) Chunks() journal.ChnksController {
	panic("not supported")
	return nil
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
