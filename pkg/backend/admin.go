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
	"github.com/logrange/logrange/pkg/journal"
)

type (
	// Admin is a backend structure used by an api implementation
	Admin struct {
		Journals *journal.Service `inject:""`
		MainCtx  context.Context  `inject:"mainCtx"`

		logger log4g.Logger
	}
)

func NewAdmin() *Admin {
	ad := new(Admin)
	ad.logger = log4g.GetLogger("backend.Admin")
	return ad
}

// Truncate provides implementation of api.Admin.Truncate() function
func (ad *Admin) Truncate(req api.TruncateRequest) (api.TruncateResult, error) {
	srcs := make([]*api.SourceTruncateResult, 0, 10)

	err := ad.Journals.Truncate(ad.MainCtx, journal.TruncateParams{
		TagsCond:   req.TagsCond,
		MinSrcSize: req.MinSrcSize,
		MaxSrcSize: req.MaxSrcSize,
		OldestTs:   req.OldestTs,
	}, func(ti journal.TruncateInfo) {
		str := &api.SourceTruncateResult{
			Tags:          ti.Tags.String(),
			RecordsBefore: ti.BeforeRecs,
			RecordsAfter:  ti.AfterRecs,
			SizeBefore:    ti.BeforeSize,
			SizeAfter:     ti.AfterSize,
			ChunksDeleted: ti.ChunksDeleted,
			Deleted:       ti.Deleted,
		}
		srcs = append(srcs, str)
	})

	return api.TruncateResult{srcs, nil}, err
}
