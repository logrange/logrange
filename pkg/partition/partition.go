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

package partition

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/logrange/pkg/tmindex"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	"github.com/logrange/range/pkg/records/journal"
	"github.com/logrange/range/pkg/utils/bytes"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/pkg/errors"
	"os"
	"sort"
	"time"
)

type (
	// Service struct provides functionality and some functions to work with partitions(LogEvent journals)
	Service struct {
		Pool      *bytes.Pool        `inject:""`
		Journals  journal.Controller `inject:""`
		TIndex    tindex.Service     `inject:""`
		MainCtx   context.Context    `inject:"mainCtx"`
		TsIndexer tmindex.TsIndexer  `inject:""`

		logger log4g.Logger
		weCh   chan WriteEvent
		tmir   *tmirebuilder
	}

	// TruncateParams allows to provide parameters for Truncate() functions
	TruncateParams struct {
		DryRun bool
		// TagsExpr contains the tags condition to select journals to be truncated
		TagsExpr *lql.Source
		// MaxSrcSize defines the upper level of a partition size, which will be truncated, if reached
		MaxSrcSize uint64
		// MinSrcSize defines the lower level of a partition size, which will not be cut if the partition will be less
		// than this parameter after truncation
		MinSrcSize uint64
		// OldestTs defines the oldest record timestamp. Chunks with records less than the parameter are candidates
		// for truncation
		OldestTs int64
		// Max Global size
		MaxDBSize uint64
	}

	// TruncateInfo describes a truncated partition information
	TruncateInfo struct {
		// LatestTs contains timestamp for latest record in the partition
		LatestTs      int64
		Tags          tag.Set
		Src           string
		BeforeSize    uint64
		AfterSize     uint64
		BeforeRecs    uint64
		AfterRecs     uint64
		ChunksDeleted int
		Deleted       bool
	}

	// WriteEvent structure contains inforamation about write event into a partition
	WriteEvent struct {
		Tags tag.Set
		//Src the partition Id
		Src      string
		StartPos journal.Pos
		EndPos   journal.Pos
	}

	// Describes a partition info
	PartitionInfo struct {
		// Tags contains the partition tags
		Tags tag.Set
		// Size contains the size of the partition
		Size uint64
		// Records contains number of records in the partition
		Records uint64

		//Journal id contains the journal identifier
		JournalId string

		// Chunks contains information about the journal chunks
		Chunks []*ChunkInfo
	}

	// PartitionsInfo contains information about partitions for the query
	PartitionsInfo struct {
		// Partitions contains list of partitions for the query (with limit and offset)
		Partitions []*PartitionInfo
		// Count contains total number of partitions found
		Count int
		// TotalSize contains summarized size of all partitions which match the criteria
		TotalSize uint64
		// TotalRecords contains summarized number of records in all matched partitions
		TotalRecords uint64
	}

	// ChunkInfo struct desribes a chunk
	ChunkInfo struct {
		// Id the chunk id
		Id chunk.Id
		// Size contains chunk size in bytes
		Size int64
		// Records contains number of records in the chunk
		Records uint32
		// MinTs contains minimal timestamp value for the chunk records
		MinTs int64
		// MaxTs contains maximal timestamp value for the chunk records
		MaxTs int64
		// Comment any supplemental info
		Comment string
	}
)

// NewService creates new service for controlling partitions
func NewService() *Service {
	s := new(Service)
	s.logger = log4g.GetLogger("logrange.partition.Service")
	s.weCh = make(chan WriteEvent, 100)
	return s
}

// Init is part of linker.Initializer
func (s *Service) Init(ctx context.Context) error {
	s.tmir = newTmirebuilder(s, 10)
	return nil
}

// Shutdown is part of linker.Shutdowner
func (s *Service) Shutdown() {
	s.tmir.close()
}

// Write performs Write operation to a partition defined by tags.
// the flag noEvent shows whether the WriteEvent should be emitted (noEvent = false) on the write operation or not
func (s *Service) Write(ctx context.Context, tags string, lit model.Iterator, noEvent bool) error {
	src, ts, err := s.TIndex.GetOrCreateJournal(tags)
	if err != nil {
		return errors.Wrapf(err, "could not parse tags %s to turn it to partition source Id.", tags)
	}

	jrnl, err := s.Journals.GetOrCreate(ctx, src)
	if err != nil {
		s.TIndex.Release(src)
		return errors.Wrapf(err, "could not get or create new partition with src=%s by tags=%s", src, tags)
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
				err = errors.Wrapf(err1, "could not write records to the partition %s by tags=%s", src, tags)
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
func (s *Service) GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]journal.Journal, error) {
	res := make(map[tag.Line]journal.Journal)

	var err1 error
	err := s.TIndex.Visit(tagsCond, func(tags tag.Set, jrnl string) bool {
		var j journal.Journal
		j, err1 = s.Journals.GetOrCreate(ctx, jrnl)
		if err1 != nil {
			s.logger.Error("GetJournals(): Could not create of get partition instance for ", jrnl, ", err=", err1)
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

// GetJournal acquires the journal by its journal Id (jn) or retruns an error if it is not possbile,
// If no errors returned, the journal must be released by Release() function after usage
func (s *Service) GetJournal(ctx context.Context, jn string) (tag.Set, journal.Journal, error) {
	ts, err := s.TIndex.GetJournalTags(jn)
	if err != nil {
		return ts, nil, errors.Wrapf(err, "GetJournal(): could not retrieve tags by s.TIndex.GetJournalTags()")
	}

	j, err := s.Journals.GetOrCreate(ctx, jn)
	if err != nil {
		s.TIndex.Release(jn)
		return ts, nil, errors.Wrapf(err, "GetJournal(): could not create journals s.Journals.GetOrCreate()")
	}

	return ts, j, nil
}

// Release releases the partition. Journal must not be used after the call. This is part of cursor.JournalsProvider
func (s *Service) Release(jn string) {
	s.TIndex.Release(jn)
}

// GetIsIndexer returns TsIndexer object
func (s *Service) GetTsIndexer() tmindex.TsIndexer {
	return s.TsIndexer
}

// TmIndexRebuilder returns the time index rebuilder
func (s *Service) GetTmIndexRebuilder() TmIndexRebuilder {
	return s.tmir
}

// Partitions function returns list of partitions with tags, that match to tagsCond with the specific offset and limit in the result
func (s *Service) Partitions(ctx context.Context, expr *lql.Source, offset, limit int) (*PartitionsInfo, error) {
	s.logger.Debug("Partitions(): ", expr.String(), " offset=", offset, ", limit=", limit)

	var opErr error
	ts := uint64(0)
	tr := uint64(0)

	var p *PartitionInfo
	parts := make([]*PartitionInfo, 0, 100)
	srtF := func(i int) bool {
		return parts[i].Size < p.Size
	}

	err := s.TIndex.Visit(expr, func(tags tag.Set, jrnl string) bool {
		j, err := s.Journals.GetOrCreate(ctx, jrnl)
		if err != nil {
			s.logger.Error("Could not create of get partition instance for ", jrnl, ", err=", err)
			opErr = err
			return false
		}
		p = &PartitionInfo{Tags: tags, Size: j.Size(), Records: j.Count()}
		idx := sort.Search(len(parts), srtF)
		parts = append(parts, p)

		if idx < len(parts)-1 {
			copy(parts[idx+1:], parts[idx:])
			parts[idx] = p
		}
		ts += p.Size
		tr += p.Records
		return true
	}, 0)

	if err != nil {
		s.logger.Warn("Partitions(): could not obtain partitions err=", err)
		return nil, err
	}

	if opErr != nil {
		s.logger.Warn("Partitions(): error in a visitor err=", err, " will report as a failure.")
		return nil, err
	}

	if limit > 1000 {
		limit = 1000
	}

	if offset >= len(parts) {
		return &PartitionsInfo{Count: len(parts), TotalSize: ts, TotalRecords: tr}, nil
	}

	sz := len(parts) - offset
	if limit > sz {
		limit = sz
	}
	res := new(PartitionsInfo)
	res.Partitions = make([]*PartitionInfo, limit)
	res.Count = len(parts)
	res.TotalSize = ts
	res.TotalRecords = tr

	for i := offset; limit > 0; i++ {
		res.Partitions[i-offset] = parts[i]
		limit--
	}

	s.logger.Debug("Partitions(): ", expr, " offset=", offset, ", limit=", limit, " found ", len(parts), " returning ", len(res.Partitions))

	return res, nil
}

// GetParitionInfo returns a partition info using its unique tags combination
func (s *Service) GetParitionInfo(tags string) (PartitionInfo, error) {
	src, ts, err := s.TIndex.GetJournal(tags)
	if err != nil {
		return PartitionInfo{}, err
	}
	defer s.TIndex.Release(src)

	jrnl, err := s.Journals.GetOrCreate(s.MainCtx, src)
	if err != nil {
		return PartitionInfo{}, err
	}

	cks, err := jrnl.Chunks().Chunks(s.MainCtx)
	if err != nil {
		return PartitionInfo{}, err
	}

	sc := s.TsIndexer.SyncChunks(s.MainCtx, jrnl.Name(), cks)
	var res PartitionInfo
	res.Chunks = make([]*ChunkInfo, len(sc))
	sz := uint64(0)
	tr := uint64(0)
	for i, c := range sc {
		ci := new(ChunkInfo)
		ci.Id = c.Id
		ci.MaxTs = c.MaxTs
		ci.MinTs = c.MinTs
		ci.Records = cks[i].Count()
		ci.Size = cks[i].Size()

		cnt, err := s.TsIndexer.Count(src, c.Id)
		if err != nil {
			s.tmir.RebuildIndex(src, c.Id, true)
			ci.Comment = "Time index was corrupted or did not exist. Rebuilding..."
		} else {
			ci.Comment = fmt.Sprintf("Found %d intervals in the time index", cnt)
		}

		sz += uint64(ci.Size)
		tr += uint64(ci.Records)
		res.Chunks[i] = ci
	}
	res.Tags = ts
	res.JournalId = src
	res.Size = sz
	res.Records = tr

	return res, nil
}

type OnTruncateF func(ti TruncateInfo)

// Truncate walks over matched journals and truncate the chunks, if needed
func (s *Service) Truncate(ctx context.Context, tp TruncateParams, otf OnTruncateF) error {
	start := time.Now()
	cremoved := 0
	ts := uint64(0)
	rs := ts

	// sortedInfos contains the list of all paritions that match the tagsExpr criteria.
	// Truncate is split onto 2 phases:
	// phase I - collect all partitions and truncate them individually. Sort all this partitions and store into sortedInfos
	// phase II - truncate the sorted list sortedInfos once again to control the global size and notify about changes
	// 			which were made either on phase I or phase II
	sortedInfos := make([]*TruncateInfo, 0, 100)

	s.logger.Debug("Trundate(): tp=", tp)

	err := s.TIndex.Visit(tp.TagsExpr, func(tags tag.Set, jn string) bool {
		s.logger.Debug("Truncate(): considering ", jn, " for tags=", tags.Line())
		j, err := s.Journals.GetOrCreate(ctx, jn)
		if err != nil {
			s.logger.Error("Truncate(): could not obtain partition by its name=", jn, ", the condition is ", tp.TagsExpr.String(), ". err=", err)
			return ctx.Err() == nil
		}

		size := j.Size()
		recs := j.Count()
		if size == 0 {
			s.logger.Debug("Truncate(): size is 0, will try to delete the partition ", jn)
			if (tp.DryRun || s.deleteJournal(ctx, j)) && otf != nil {
				otf(TruncateInfo{0, tags, jn, 0, 0, 0, 0, 0, true})
			}
			return ctx.Err() == nil
		}

		ts += size
		n, tr, err := s.truncate(ctx, j, &tp)
		if err != nil {
			s.logger.Error("Truncate(): could not truncate data from the partition ", jn, ", err=", err)
		} else {
			rs += tr
			cremoved += n
		}
		arecs := j.Count()

		deleted := false
		if tr == size {
			s.logger.Debug("Truncate(): size is 0 after truncation, will try to delete ", jn)
			deleted = tp.DryRun || s.deleteJournal(ctx, j)
		}

		if err == nil {
			lci, err := s.TsIndexer.LastChunkRecordsInfo(jn)
			lts := int64(0)
			if err == nil {
				lts = lci.MaxTs
			}
			ti := &TruncateInfo{lts, tags, jn, size, size - tr, recs, arecs, n, deleted}
			idx := sort.Search(len(sortedInfos), func(idx int) bool {
				return sortedInfos[idx].LatestTs <= ti.LatestTs
			})
			sortedInfos = append(sortedInfos, ti)
			if idx < len(sortedInfos)-1 {
				copy(sortedInfos[idx+1:], sortedInfos[idx:len(sortedInfos)-1])
				sortedInfos[idx] = ti
			}
		}

		return ctx.Err() == nil
	}, tindex.VF_SKIP_IF_LOCKED)

	cremoved2, rs2 := s.truncateGlobally(ctx, sortedInfos, &tp, otf)

	if err != nil {
		s.logger.Warn("Truncate(): an error happened while visiting the journals, err=", err)
		return err
	}

	cremoved += cremoved2
	rs += rs2

	s.logger.Info("Truncate(dryrun=", tp.DryRun, "): ", cremoved, " chunk(s) were removed. Initial size of ALL journals was ", humanize.Bytes(ts),
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

// truncateGlobally deletes journals using sortedInfos till total size exceeds the specified value.
func (s *Service) truncateGlobally(ctx context.Context, sortedInfos []*TruncateInfo, tp *TruncateParams, otf OnTruncateF) (int, uint64) {
	// First count the global size
	ts := uint64(0)
	for _, ti := range sortedInfos {
		ts += ti.AfterSize
	}

	s.logger.Info("truncateGlobally(): the total size for ", len(sortedInfos), " partitions is ", ts, ", but max size is set to ", tp.MaxDBSize)

	cr := 0
	tr := uint64(0)
	jrnls := 0

	// Now, remove all journals if the DB size is exceeded
	for i := 0; i < len(sortedInfos) && ts > tp.MaxDBSize; i++ {
		ti := sortedInfos[i]
		if ti.AfterSize > 0 {
			_, err := s.TIndex.GetJournalTags(ti.Src)
			if err != nil {
				s.logger.Warn("Could not acquire journal ", ti.Src, " for deletion err=", err)
				continue
			}

			j, err := s.Journals.GetOrCreate(ctx, ti.Src)
			if err != nil {
				s.TIndex.Release(ti.Src)
				s.logger.Warn("Could not get journal ", ti.Src, " for deletion err=", err)
				continue
			}

			cks, _ := j.Chunks().Chunks(ctx)
			deleted := tp.DryRun || s.deleteJournal(ctx, j)
			s.TIndex.Release(ti.Src)
			if deleted {
				jrnls++
				ts -= ti.AfterSize
				tr += ti.AfterSize
				cr += len(cks)
				ti.AfterSize = 0
				ti.AfterRecs = 0
				ti.ChunksDeleted += len(cks)
				ti.Deleted = true
			}
		}
	}

	if tp.DryRun {
		s.logger.Info("truncateGlobally(): (dryrun) ", jrnls, " journals would be deleted. ", cr, " chunks and ", tr, " bytes of data could be affected")
	} else {
		s.logger.Info("truncateGlobally(): ", jrnls, " journals were deleted. ", cr, " chunks and ", tr, " bytes of data")
	}

	// Notify the listener, if needed
	if otf != nil {
		for _, ti := range sortedInfos {
			if ti.AfterSize != ti.BeforeSize {
				otf(*ti)
			}
		}
	}

	return cr, tr
}

// truncate receives a partition and the truncate params, truncate the partition and returns number of chunks truncated,
// together with total size of removed chunks. It returns an error if the operation was unsuccessful
func (s *Service) truncate(ctx context.Context, jrnl journal.Journal, tp *TruncateParams) (int, uint64, error) {
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
		sc := s.TsIndexer.SyncChunks(ctx, jrnl.Name(), cks)
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

func (s *Service) deleteJournal(ctx context.Context, j journal.Journal) bool {
	jn := j.Name()
	s.logger.Debug("deleting the partition ", jn, " completely.")
	if !s.TIndex.LockExclusively(jn) {
		s.logger.Warn("deleteJournal(): could not acquire the partition ", jn, " exlusively. Giving up.")
		return false
	}

	if sz := j.Size(); sz > 0 {
		s.TIndex.UnlockExclusively(jn)
		s.logger.Warn("deleteJournal(): could not delete the partition ", jn, " the size is not 0: ", sz)
		return false
	}

	dir := j.Chunks().LocalFolder()

	err := s.TIndex.Delete(jn)
	s.TIndex.UnlockExclusively(jn)
	if err != nil {
		s.logger.Warn("deleteJournal(): could not delete the partition ", jn, " err=", err)
		return false
	}

	if len(dir) > 0 {
		err = os.RemoveAll(dir)
		if err != nil {
			s.logger.Error("deleteJournal(): could not remove folder ", dir, " for the partition ", jn, " err=", err)
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

func (s *Service) onWriteCIndex(src string, iw *iwrapper, n int, pos journal.Pos) {
	ri := tmindex.RecordsInfo{pos.CId, iw.minTs, iw.maxTs}
	err := s.TsIndexer.OnWrite(src, pos.Idx-uint32(n), pos.Idx-1, ri)
	if err == tmindex.ErrTmIndexCorrupted {
		// if no time-index kick the rebuilder to make it
		s.tmir.RebuildIndex(src, ri.Id, false)
	}
}

func (tp TruncateParams) String() string {
	return fmt.Sprintf("{DryRun=%t, TagsCond=%s, MaxSrcSize=%d, MinSrcSize=%d, OldestTs=%d}", tp.DryRun, tp.TagsExpr.String(), tp.MaxSrcSize, tp.MinSrcSize, tp.OldestTs)
}
