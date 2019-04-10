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
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/partition"
	"github.com/logrange/logrange/pkg/utils"
	"math"
	"strings"
)

type (
	// Admin is a backend structure used by an api implementation
	Admin struct {
		Partitions *partition.Service `inject:""`
		MainCtx    context.Context    `inject:"mainCtx"`

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

	err := ad.Partitions.Truncate(ad.MainCtx, partition.TruncateParams{
		DryRun:     req.DryRun,
		TagsCond:   req.TagsCond,
		MinSrcSize: req.MinSrcSize,
		MaxSrcSize: req.MaxSrcSize,
		OldestTs:   req.OldestTs,
	}, func(ti partition.TruncateInfo) {
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

// Execute provides implementation of api.Admin.Execute() function
func (ad *Admin) Execute(req api.ExecRequest) (api.ExecResult, error) {
	ad.logger.Debug("Executing ", req.Query)
	l, err := lql.ParseLql(req.Query)
	if err != nil {
		ad.logger.Warn("Wrong query provided: ", req.Query, ", err=", err)
		return api.ExecResult{}, err
	}

	if l.Show != nil {
		return ad.cmdShow(l.Show)
	}

	return api.ExecResult{}, fmt.Errorf("Could not execute(%s) via the call. Please use dedicated API for the kind of queries", req.Query)
}

func (ad *Admin) cmdShow(s *lql.Show) (api.ExecResult, error) {
	offset := utils.GetIntVal(s.Partitions.Offset, 0)
	limit := utils.GetIntVal(s.Partitions.Limit, int(math.MaxUint32))
	parts, err := ad.Partitions.Partitions(ad.MainCtx, s.Partitions.Source, offset, limit)
	if err != nil {
		return api.ExecResult{}, err
	}

	// formatting the result for human redability
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d partitions (starting from offset=%d):\n", len(parts.Partitions), offset))

	first := true
	for _, s := range parts.Partitions {
		if first {
			sb.WriteString(fmt.Sprintf("\n%10s  %13s  %s", "SIZE", "RECORDS", "TAGS"))
			sb.WriteString("\n----------  -------------  ----")
			first = false
		}

		sb.WriteString(fmt.Sprintf("\n%10s %13s  %s", humanize.Bytes(s.Size), humanize.Comma(int64(s.Records)), s.Tags.String()))
	}

	if !first {
		if len(parts.Partitions)+offset < parts.Count {
			sb.WriteString(fmt.Sprintf("\n\n... and %d more not shown here...\n", parts.Count-len(parts.Partitions)))
		}

		if parts.Count > 1 {
			sb.WriteString("\n----------  -------------")
			sb.WriteString(fmt.Sprintf("\n%10s  %13s\n", humanize.Bytes(parts.TotalSize), humanize.Comma(int64(parts.TotalRecords))))
		}
	}

	sb.WriteString(fmt.Sprintf("\ntotal: %d sources match the criteria", parts.Count))
	return api.ExecResult{Output: sb.String()}, nil
}
