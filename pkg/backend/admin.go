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

package backend

import (
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	"github.com/logrange/range/pkg/records/journal"
	"math"
	"os"
)

type (
	Admin struct {
		TIndex   tindex.Service     `inject:""`
		Journals journal.Controller `inject:""`
		MainCtx  context.Context    `inject:"mainCtx"`

		logger log4g.Logger
	}
)

func NewAdmin() *Admin {
	ad := new(Admin)
	ad.logger = log4g.GetLogger("backend.Admin")
	return ad
}

func removeChunkFile(cid chunk.Id, filename string, err error) {
	if err != nil {
		return
	}

	os.Remove(chunkfs.SetChunkDataFileExt(filename))
	os.Remove(chunkfs.SetChunkIdxFileExt(filename))
}

func (ad *Admin) Truncate(tagsCond string, maxSize uint64) (api.TruncateResult, error) {
	se, err := lql.ParseSource(tagsCond)
	if err != nil {
		ad.logger.Warn("Could not parse condition ", tagsCond, ", err=", err)
		return api.TruncateResult{}, err
	}

	// select journals first
	mp, _, err := ad.TIndex.GetJournals(se, int(math.MaxInt32), true)
	if err != nil {
		ad.logger.Warn("could not get list of sources for ", tagsCond, ", err=", err)
		return api.TruncateResult{}, err
	}

	var res api.TruncateResult

	for tags, src := range mp {
		j, err := ad.Journals.GetOrCreate(ad.MainCtx, src)
		if err != nil {
			ad.logger.Warn("Truncate(): could not get the source=", src, ", err=", err)
			continue
		}
		sb := j.Size()
		cb := j.Count()
		ccnt, err := j.Truncate(ad.MainCtx, maxSize, removeChunkFile)
		if err != nil {
			ad.logger.Error("Truncate(): could not truncate source ", src, " err=", err)
		}
		if ccnt == 0 {
			continue
		}

		if res.Sources == nil {
			res.Sources = make([]*api.SourceTruncateResult, 0, 10)
		}
		res.Sources = append(res.Sources, &api.SourceTruncateResult{Tags: string(tags), SizeBefore: uint64(sb), RecordsBefore: cb, SizeAfter: uint64(j.Size()), RecordsAfter: j.Count()})
	}

	return res, nil
}
