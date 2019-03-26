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

package journal

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	rjournal "github.com/logrange/range/pkg/records/journal"
	"github.com/logrange/range/pkg/utils/bytes"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/pkg/errors"
	"os"
	"sort"
	"time"
)

type (
	// Service struct provides functionality and some functions to work with LogEvent journals
	Service struct {
		Pool     *bytes.Pool         `inject:""`
		Journals rjournal.Controller `inject:""`
		TIndex   tindex.Service      `inject:""`
		CIdxDir  string              `inject:"cindexDir"`
		MainCtx  context.Context     `inject:"mainCtx"`

		cIdx   *cindex
		logger log4g.Logger
		weCh   chan WriteEvent
	}

	// TruncateParams allows to provide parameters for Truncate() functions
	TruncateParams struct {
		DryRun bool
		// TagsCond contains the tags condition to select journals to be truncated
		TagsCond string
		// MaxSrcSize defines the upper level of a journal size, which will be truncated, if reached
		MaxSrcSize uint64
		// MinSrcSize defines the lower level of a journal size, which will not be cut if the journal will be less
		// than this parameter after truncation
		MinSrcSize uint64
		// OldestTs defines the oldest record timestamp. Chunks with records less than the parameter are candidates
		// for truncation
		OldestTs uint64
	}

	TruncateInfo struct {
		Tags          tag.Set
		Src           string
		BeforeSize    uint64
		AfterSize     uint64
		BeforeRecs    uint64
		AfterRecs     uint64
		ChunksDeleted int
		Deleted       bool
	}

	// WriteEvent structure contains inforamation about write event into a journal
	WriteEvent struct {
		Tags     tag.Set
		Src      string
		StartPos rjournal.Pos
		EndPos   rjournal.Pos
	}
)

func NewService() *Service {
	s := new(Service)
	s.cIdx = newCIndex()
	s.logger = log4g.GetLogger("logrange.journal.Service")
	s.weCh = make(chan WriteEvent, 100)
	return s
}

func (s *Service) Init(ctx context.Context) error {
	s.logger.Info("Initializing")
	s.cIdx.init(s.CIdxDir)
	return nil
}

func (s *Service) Shutdown() {
	s.logger.Info("Shutdown()")
	s.cIdx.saveDataToFile()
}

// Write performs Write operation to a journal defined by tags.
// the flag noEvent shows whether the WriteEvent should be emitted (noEvent = false) on the write operation or not
func (s *Service) Write(ctx context.Context, tags string, lit model.Iterator, noEvent bool) error {
	src, ts, err := s.TIndex.GetOrCreateJournal(tags)
	if err != nil {
		return errors.Wrapf(err, "could not parse tags %s to turn it to journal source Id.", tags)
	}

	jrnl, err := s.Journals.GetOrCreate(ctx, src)
	if err != nil {
		s.TIndex.Release(src)
		return errors.Wrapf(err, "could not get or create new journal with src=%s by tags=%s", src, tags)
	}

	var iw iwrapper
	iw.pool = s.Pool
	iw.it = lit

	var we WriteEvent
	weInit := false

	for {
		n, pos, err1 := jrnl.Write(ctx, &iw)
		if n > 0 {
			s.onWriteCIndex(src, &iw, n, pos)
			if !weInit {
				we.Tags = ts
				we.Src = src
				we.StartPos = pos
				we.StartPos.Idx -= uint32(n)
			}
			weInit = true
			we.EndPos = pos
		}

		if err1 != nil {
			if n <= 0 {
				err = errors.Wrapf(err1, "could not write records to the journal %s by tags=%s", src, tags)
			}
			break
		}
		if _, err1 = iw.Get(ctx); err1 != nil {
			break
		}
	}

	if weInit && !noEvent {
		s.onWriteEvent(we)
	}

	s.TIndex.Release(src)
	iw.close() // free resources
	return err
}

// GetJournals is part of cursor.JournalsProvider
func (s *Service) GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]rjournal.Journal, error) {
	var res map[tag.Line]rjournal.Journal
	if maxLimit < 100 {
		res = make(map[tag.Line]rjournal.Journal)
	} else {
		res = make(map[tag.Line]rjournal.Journal)
	}

	var err1 error
	err := s.TIndex.Visit(tagsCond, func(tags tag.Set, jrnl string) bool {
		var j rjournal.Journal
		j, err1 = s.Journals.GetOrCreate(ctx, jrnl)
		if err1 != nil {
			s.logger.Error("GetJournals(): Could not create of get journal instance for ", jrnl, ", err=", err1)
			return false
		}

		if len(res) == maxLimit {
			s.TIndex.Release(jrnl)
			err1 = errors.Errorf("Limit exceeds. Expected no more than %d journals, but at least %d alredy found ", maxLimit, maxLimit+1)
			return false
		}

		res[tags.Line()] = j

		return true
	}, tindex.VF_DO_NOT_RELEASE)

	if err1 != nil || err != nil {
		for _, j := range res {
			s.TIndex.Release(j.Name())
		}
		res = nil
		if err1 != nil {
			err = err1
		}
	}

	return res, err
}

// Release releases the journal. Journal must not be used after the call. This is part of cursor.JournalsProvider
func (s *Service) Release(jn string) {
	s.TIndex.Release(jn)
}

// Sources provides implementation for api.Querier.Source function
func (s *Service) Sources(ctx context.Context, tagsCond string) (*api.SourcesResult, error) {
	srcExp, err := lql.ParseSource(tagsCond)
	if err != nil {
		s.logger.Warn("Sources(): could not parse the source condition ", tagsCond, " err=", err)
		return nil, err
	}

	srcs := make([]api.Source, 0, 100)
	ts := uint64(0)
	tr := uint64(0)
	count := 0
	var opErr error
	var src api.Source

	// by default sort by descending
	srtF := func(i int) bool {
		return srcs[i].Size < src.Size
	}

	err = s.TIndex.Visit(srcExp, func(tags tag.Set, jrnl string) bool {
		count++
		j, err := s.Journals.GetOrCreate(ctx, jrnl)
		if err != nil {
			s.logger.Error("Could not create of get journal instance for ", jrnl, ", err=", err)
			opErr = err
			return false
		}
		ts += j.Size()
		tr += j.Count()
		src.Tags = tags.Line().String()
		src.Size = j.Size()
		src.Records = j.Count()

		idx := sort.Search(len(srcs), srtF)
		szBefore := len(srcs)
		if len(srcs) < cap(srcs) {
			srcs = append(srcs, src)
		}

		if idx < szBefore {
			copy(srcs[idx+1:], srcs[idx:])
			srcs[idx] = src
		}
		return true
	}, 0)

	if err != nil {
		s.logger.Warn("Sources(): could not obtain sources err=", err)
		return nil, err
	}

	if opErr != nil {
		s.logger.Warn("Sources(): error in a visitor err=", err, " will report as a failure.")
		return nil, err
	}

	s.logger.Debug("Requested journals for ", tagsCond, ". returned ", len(srcs), " in map with total count=", count)

	return &api.SourcesResult{Sources: srcs, Count: count, TotalSize: ts, TotalRec: tr}, nil
}

type OnTruncateF func(ti TruncateInfo)

// TruncateBySize walks over all journals and prune them if the size exceeds maximum value
func (s *Service) Truncate(ctx context.Context, tp TruncateParams, otf OnTruncateF) error {
	srcExp, err := lql.ParseSource(tp.TagsCond)
	if err != nil {
		s.logger.Warn("Truncate(): could not parse the source condition ", tp.TagsCond, " err=", err)
		return err
	}

	start := time.Now()
	cremoved := 0
	ts := uint64(0)
	rs := ts

	s.logger.Debug("Trundate(): tp=", tp)

	err = s.TIndex.Visit(srcExp, func(tags tag.Set, jn string) bool {
		s.logger.Debug("Truncate(): considering ", jn, " for tags=", tags.Line())
		j, err := s.Journals.GetOrCreate(ctx, jn)
		if err != nil {
			s.logger.Error("Truncate(): could not obtain journal by its name=", jn, ", the condition is ", tp.TagsCond, ". err=", err)
			return ctx.Err() == nil
		}

		size := j.Size()
		recs := j.Count()
		if size == 0 {
			s.logger.Debug("Truncate(): size is 0, will try to delete the journal ", jn)
			if (tp.DryRun || s.deleteJournal(ctx, j)) && otf != nil {
				otf(TruncateInfo{tags, jn, 0, 0, 0, 0, 0, true})
			}
			return ctx.Err() == nil
		}

		ts += size
		n, tr, err := s.truncate(ctx, j, &tp)
		if err != nil {
			s.logger.Error("Truncate(): could not truncate data from the journal ", jn, ", err=", err)
		} else {
			ts += tr
			cremoved += n
		}
		arecs := j.Count()

		deleted := false
		if tr == size {
			s.logger.Debug("Truncate(): size is 0 after truncation, will try to delete ", jn)
			deleted = tp.DryRun || s.deleteJournal(ctx, j)
		}

		if err == nil && otf != nil && tr > 0 {
			otf(TruncateInfo{tags, jn, size, size - tr, recs, arecs, n, deleted})
		}

		return ctx.Err() == nil
	}, tindex.VF_SKIP_IF_LOCKED)

	if err != nil {
		s.logger.Warn("Truncate(): an error happened while visiting the journals, err=", err)
		return err
	}

	s.logger.Info("Truncate(): ", cremoved, " chunk(s) were removed. Initial size of ALL journals was ", humanize.Bytes(ts),
		"(", ts, "), and ", humanize.Bytes(rs), "(", rs, ") were removed. It took ", time.Now().Sub(start))
	return nil
}

// GetWriteEvent reads next write event. It blocks caller until the contex is closed or new event comes
func (s *Service) GetWriteEvent(ctx context.Context) (WriteEvent, error) {
	select {
	case <-s.MainCtx.Done():
		return WriteEvent{}, s.MainCtx.Err()
	case <-ctx.Done():
		return WriteEvent{}, ctx.Err()
	case we, ok := <-s.weCh:
		if !ok {
			return WriteEvent{}, errors2.ClosedState
		}
		return we, nil
	}
}

func (s *Service) onWriteEvent(we WriteEvent) {
	select {
	case s.weCh <- we:
	case <-s.MainCtx.Done():
		s.logger.Warn("onWriteEvent(): Unblock a notirifcation")
	}
}

// truncate receives a journal and the truncate params, truncate the journal and returns number of chunks truncated,
// together with total size of removed chunks. It returns an error if the operation was unsuccessful
func (s *Service) truncate(ctx context.Context, jrnl rjournal.Journal, tp *TruncateParams) (int, uint64, error) {
	cks, err := jrnl.Chunks().Chunks(ctx)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "truncate(): could not read list of chunks for %s", jrnl.Name())
	}

	size := jrnl.Size()
	isize := size
	idx := 0
	s.logger.Debug("truncate(): ", jrnl, " has ", len(cks), " chunks, with total size=", size)

	// first cut by the size, idx is exclusive
	if tp.MaxSrcSize > 0 && tp.MaxSrcSize > tp.MinSrcSize {
		for ; idx < len(cks) && size > tp.MaxSrcSize && size-uint64(cks[idx].Size()) >= tp.MinSrcSize; idx++ {
			size -= uint64(cks[idx].Size())
		}
	}
	s.logger.Debug("After checking size first ", idx, " chunks considered to be removed. New size=", size)

	if tp.OldestTs > 0 && idx < len(cks) {
		sc := s.cIdx.syncChunks(ctx, jrnl.Name(), cks)
		for ; idx < len(sc) && sc[idx].MaxTs <= tp.OldestTs && size-uint64(cks[idx].Size()) >= tp.MinSrcSize; idx++ {
			size -= uint64(cks[idx].Size())
		}
	}
	s.logger.Debug("After checking records' time, first ", idx, " chunks considered to be removed. New size=", size)

	n := idx
	idx--
	if idx < 0 || tp.DryRun {
		return n, isize - size, nil
	}

	n, err = jrnl.Chunks().DeleteChunks(ctx, cks[idx].Id(), func(cid chunk.Id, filename string, err error) {
		s.deleteChunk(filename)
	})
	if err != nil {
		return 0, 0, errors.Wrapf(err, "truncate(): Could not truncate chunks for %v", jrnl)
	}
	return n, isize - size, nil
}

func (s *Service) deleteJournal(ctx context.Context, j rjournal.Journal) bool {
	jn := j.Name()
	s.logger.Debug("deleting the journal ", jn, " completely.")
	if !s.TIndex.LockExclusively(jn) {
		s.logger.Warn("deleteJournal(): could not acquire the journal ", jn, " exlusively. Giving up.")
		return false
	}

	if sz := j.Size(); sz > 0 {
		s.TIndex.UnlockExclusively(jn)
		s.logger.Warn("deleteJournal(): could not delete the journal ", jn, " the size is not 0: ", sz)
		return false
	}

	dir := j.Chunks().LocalFolder()

	err := s.TIndex.Delete(jn)
	s.TIndex.UnlockExclusively(jn)
	if err != nil {
		s.logger.Warn("deleteJournal(): could not delete the journal ", jn, " err=", err)
		return false
	}

	if len(dir) > 0 {
		err = os.RemoveAll(dir)
		if err != nil {
			s.logger.Error("deleteJournal(): could not remove folder ", dir, " for the journal ", jn, " err=", err)
		}
	}
	return true
}

func (s *Service) deleteChunk(cfname string) {
	fn := chunkfs.SetChunkDataFileExt(cfname)
	s.logger.Debug("deleteChunk(): Deleting ", fn)
	if err := os.Remove(fn); err != nil && !os.IsNotExist(err) {
		s.logger.Warn("could not remove the chunk file ", fn, ", err=", err)
	}

	fn = chunkfs.SetChunkIdxFileExt(cfname)
	s.logger.Debug("deleteChunk(): Deleting ", fn)
	if err := os.Remove(fn); err != nil {
		s.logger.Warn("could not remove the chunk file ", fn, ", err=", err)
	}
}

func (s *Service) onWriteCIndex(src string, iw *iwrapper, n int, pos rjournal.Pos) {
	ci := chkInfo{pos.CId, iw.minTs, iw.maxTs}
	s.cIdx.onWrite(src, pos.Idx-uint32(n), pos.Idx-1, ci)
}

func (tp TruncateParams) String() string {
	return fmt.Sprintf("{DryRun=%t, TagsCond=%s, MaxSrcSize=%d, MinSrcSize=%d, OldestTs=%d}", tp.DryRun, tp.TagsCond, tp.MaxSrcSize, tp.MinSrcSize, tp.OldestTs)
}
