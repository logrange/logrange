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
	"github.com/dustin/go-humanize"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	rjournal "github.com/logrange/range/pkg/records/journal"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/pkg/errors"
	"os"
	"time"
)

type (
	// Service struct provides functionality and some functions to work with LogEvent journals
	Service struct {
		Pool     *bytes.Pool         `inject:""`
		Journals rjournal.Controller `inject:""`
		TIndex   tindex.Service      `inject:""`
		CIdxDir  string              `inject:"cindexDir"`

		cIdx   *cindex
		logger log4g.Logger
	}
)

func NewService() *Service {
	s := new(Service)
	s.cIdx = newCIndex()
	s.logger = log4g.GetLogger("logrange.journal.Service")
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
func (s *Service) Write(ctx context.Context, tags string, lit model.Iterator) error {
	src, err := s.TIndex.GetOrCreateJournal(tags)
	if err != nil {
		return errors.Wrapf(err, "could not parse tags %s to turn it to journal source Id.", tags)
	}

	jrnl, err := s.Journals.GetOrCreate(ctx, src)
	if err != nil {
		return errors.Wrapf(err, "could not get or create new journal with src=%s by tags=%s", src, tags)
	}

	var iw iwrapper
	iw.pool = s.Pool
	iw.it = lit
	n, pos, err := jrnl.Write(ctx, &iw)
	if err != nil {
		return errors.Wrapf(err, "could not write records to the journal %s by tags=%s", src, tags)
	}

	if n > 0 {
		s.onWriteCIndex(src, &iw, n, pos)
	}

	iw.close() // free resources
	return nil
}

// TruncateBySize walks over all journals and prune them if the size exceeds maximum value
func (s *Service) TruncateBySize(ctx context.Context, maxSize uint64) {
	start := time.Now()
	cremoved := 0
	ts := uint64(0)
	rs := ts
	s.Journals.Visit(ctx, func(j rjournal.Journal) bool {
		size := j.Size()
		ts += size
		if size > maxSize {
			cks, err := j.Chunks().Chunks(ctx)
			if err == nil {
				idx := 0
				isize := size
				for _, ck := range cks {
					size -= uint64(ck.Size())
					if size <= maxSize {
						break
					}
					idx++
				}

				if idx == len(cks) {
					idx = len(cks) - 1
				}
				n, err := j.Chunks().DeleteChunks(ctx, cks[idx].Id(), func(cid chunk.Id, filename string, err error) {
					s.deleteChunk(filename)
				})
				if err != nil {
					s.logger.Error("Could not remove chunks for ", j, ", err=", err)
				} else {
					rs += isize - size
					cremoved += n
				}
			}
		}
		return ctx.Err() == nil
	})

	s.logger.Info("TruncateBySize(): ", cremoved, " chunks removed. Initial size was ", humanize.Bytes(ts),
		"(", ts, "), removed ", humanize.Bytes(rs), "(", rs, "). It took ", time.Now().Sub(start))
}

// TruncateByTimestamp checks all journals and deletes chunks, that contain information earlier than uNano, but
// only if the journal size will not become LESS than szThreshold
func (s *Service) TruncateByTimestamp(ctx context.Context, uNano uint64, szThreshold uint64) {
	start := time.Now()
	ts := uint64(0)
	tr := uint64(0)
	cremoved := 0
	s.Journals.Visit(ctx, func(j rjournal.Journal) bool {
		js := j.Size()
		red := uint64(0)
		if js > szThreshold {
			cks, err := j.Chunks().Chunks(ctx)
			if err == nil {
				sc := s.cIdx.syncChunks(ctx, j.Name(), cks)
				idx := -1
				for i, ck := range cks {
					if sc[i].MaxTs > uNano || js-red < szThreshold {
						break
					}
					idx = i
					red += uint64(ck.Size())
				}

				if idx >= 0 {
					n, err := j.Chunks().DeleteChunks(ctx, cks[idx].Id(), func(cid chunk.Id, filename string, err error) {
						s.deleteChunk(filename)
					})
					if err != nil {
						red = 0
						s.logger.Error("Could not remove chunks for ", j, ", err=", err)
					} else {
						cremoved += n
					}
				}
			}
		}
		ts += js
		tr += red
		return ctx.Err() == nil
	})
	s.logger.Info("TruncateByTimestamp(): ", cremoved, " chunks removed. Initial size was ", humanize.Bytes(ts),
		"(", ts, "), removed ", humanize.Bytes(tr), "(", tr, "). It took ", time.Now().Sub(start))
}

func (s *Service) deleteChunk(cfname string) {
	fn := chunkfs.SetChunkDataFileExt(cfname)
	if err := os.Remove(fn); err != nil && !os.IsNotExist(err) {
		s.logger.Warn("could not remove the chunk file ", fn, ", err=", err)
	}

	fn = chunkfs.SetChunkIdxFileExt(cfname)
	if err := os.Remove(fn); err != nil {
		s.logger.Warn("could not remove the chunk file ", fn, ", err=", err)
	}
}

func (s *Service) onWriteCIndex(src string, iw *iwrapper, n int, pos rjournal.Pos) {
	ci := chkInfo{pos.CId, iw.minTs, iw.maxTs}
	s.cIdx.onWrite(src, pos.Idx-uint32(n), pos.Idx-1, ci)
}
