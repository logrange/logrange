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
	"github.com/logrange/logrange/pkg/pipe"
	"github.com/logrange/logrange/pkg/utils"
	"math"
	"strings"
	"time"
)

type (
	// Admin is a backend structure used by an api implementation
	Admin struct {
		Partitions *partition.Service `inject:""`
		MainCtx    context.Context    `inject:"mainCtx"`
		Streams    *pipe.Service      `inject:""`

		logger log4g.Logger
	}
)

func NewAdmin() *Admin {
	ad := new(Admin)
	ad.logger = log4g.GetLogger("backend.Admin")
	return ad
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

	if l.Truncate != nil {
		return ad.cmdTruncate(l.Truncate)
	}

	if l.Create != nil && l.Create.Pipe != nil {
		return ad.cmdCreatePipe(l.Create.Pipe)
	}

	if l.Delete != nil && l.Delete.PipeName != nil {
		return ad.cmdDeletePipe(*l.Delete.PipeName)
	}

	if l.Describe != nil {
		return ad.cmdDescribe(l.Describe)
	}

	return api.ExecResult{}, fmt.Errorf("Could not execute(%s) via the call. Please use dedicated API for the kind of queries", req.Query)
}

func (ad *Admin) cmdShow(s *lql.Show) (api.ExecResult, error) {
	if s.Partitions != nil {
		return ad.cmdShowPartitions(s.Partitions)
	}

	if s.Pipes != nil {
		return ad.cmdShowPipes(s.Pipes)
	}

	return api.ExecResult{}, fmt.Errorf("Could not execute show command(%s), not supported.", s.String())
}

func (ad *Admin) cmdShowPartitions(p *lql.Partitions) (api.ExecResult, error) {
	offset := utils.GetIntVal(p.Offset, 0)
	limit := utils.GetIntVal(p.Limit, int(math.MaxUint32))
	parts, err := ad.Partitions.Partitions(ad.MainCtx, p.Source, offset, limit)
	if err != nil {
		return api.ExecResult{}, err
	}

	// formatting the result for human redability
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d partitions (starting with offset=%d):\n", len(parts.Partitions), offset))

	ad.logger.Info("parts= ", parts)

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

func (ad *Admin) cmdShowPipes(p *lql.Pipes) (api.ExecResult, error) {
	lim := int(utils.GetInt64Val(p.Limit, 0))
	if lim == 0 {
		lim = math.MaxInt32
	}
	offs := int(utils.GetInt64Val(p.Offset, 0))
	stms := ad.Streams.GetPipes()
	var sb strings.Builder
	if lim+offs > len(stms) {
		lim = len(stms) - offs
		if lim < 0 {
			lim = 0
		}
	}

	sb.WriteString(fmt.Sprintf("%d pipes found. %d pipes (starting with offset=%d):\n", len(stms), lim, offs))
	for i := offs; i < len(stms) && lim > 0; i++ {
		sb.WriteString(stms[i].Name)
		sb.WriteByte('\n')
		lim--
	}

	return api.ExecResult{Output: sb.String()}, nil
}

func (ad *Admin) cmdTruncate(t *lql.Truncate) (api.ExecResult, error) {
	first := true
	var sb strings.Builder
	cnt := 0
	err := ad.Partitions.Truncate(ad.MainCtx, partition.TruncateParams{
		DryRun:     t.DryRun,
		TagsExpr:   t.Source,
		MinSrcSize: utils.GetUint64Val((*uint64)(t.MinSize), 0),
		MaxSrcSize: utils.GetUint64Val((*uint64)(t.MaxSize), 0),
		OldestTs:   utils.GetInt64Val((*int64)(t.Before), 0),
		MaxDBSize:  utils.GetUint64Val((*uint64)(t.MaxDbSize), math.MaxUint64),
	}, func(ti partition.TruncateInfo) {
		if first {
			sb.WriteString(fmt.Sprintf("\n%20s  %20s  %9s %s", "SIZE(diff)", "RECORDS(diff)", "CHKS(ALL)", "TAGS"))
			sb.WriteString(fmt.Sprintf("\n--------------------  --------------------  ---------  ----"))
			first = false
		}

		sz := fmt.Sprintf("%s(%s)", humanize.Bytes(ti.AfterSize),
			humanize.Bytes(ti.BeforeSize-ti.AfterSize))
		recs := fmt.Sprintf("%s(%s)", humanize.Comma(int64(ti.AfterRecs)),
			humanize.Comma(int64(ti.BeforeRecs-ti.AfterRecs)))

		var cks string
		if ti.Deleted {
			cks = fmt.Sprintf("%d(YES)", ti.ChunksDeleted)
		} else {
			cks = fmt.Sprintf("%d(NO)", ti.ChunksDeleted)
		}

		sb.WriteString(fmt.Sprintf("\n%20s  %20s  %9s  %s", sz, recs, cks, ti.Tags.String()))
		cnt++
	})

	if err != nil {
		ad.logger.Warn("Could not complete truncation, for ", t.String(), ", cnt=", cnt, ", err=", err)
		sb.WriteString(fmt.Sprintf("\nUnexpected error while truncation: %s", err))
	}
	sb.WriteString(fmt.Sprintf("\n\n%d source(s) affected. \n", cnt))

	if err != nil && cnt == 0 {
		return api.ExecResult{}, err
	}
	return api.ExecResult{Output: sb.String()}, nil
}

func (ad *Admin) cmdCreatePipe(p *lql.Pipe) (api.ExecResult, error) {
	st := pipe.Pipe{Name: p.Name, TagsCond: p.From.String(), FltCond: p.Where.String()}
	_, err := ad.Streams.CreatePipe(st)
	if err != nil {
		ad.logger.Warn("cmdCreatePipe(", p.String(), "): err=", err)
		return api.ExecResult{}, err
	}

	ad.logger.Info("New pipe ", p.String(), " created")
	return api.ExecResult{Output: fmt.Sprintf("\nNew pipe with name \"%s\" has just been created.\n", st.Name)}, nil
}

func (ad *Admin) cmdDeletePipe(name string) (api.ExecResult, error) {
	err := ad.Streams.DeletePipe(name)
	if err != nil {
		ad.logger.Warn("cmdDeletePipe(", name, "): err=", err)
		return api.ExecResult{}, err
	}
	ad.logger.Info("The pipe \"", name, "\" deleted")
	return api.ExecResult{Output: fmt.Sprintf("\nThe pipe \"%s\" has just been deleted.\n", name)}, nil
}

func (ad *Admin) cmdDescribe(d *lql.Describe) (api.ExecResult, error) {
	if d.Pipe != nil {
		return ad.cmdDescribePipe(*d.Pipe)
	}

	if d.Partition != nil {
		return ad.cmdDescribePartition(d.Partition)
	}

	return api.ExecResult{}, fmt.Errorf("Unexpected describe command")
}

func (ad *Admin) cmdDescribePipe(pname string) (api.ExecResult, error) {
	sd, err := ad.Streams.GetPipe(pname)
	if err != nil {
		ad.logger.Warn("cmdDescribePipe(", pname, "): err=", err)
		return api.ExecResult{}, err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\nPipe:      %s", pname))
	sb.WriteString(fmt.Sprintf("\nFrom:      %s", sd.TagsCond))
	sb.WriteString(fmt.Sprintf("\nWhere:     %s", sd.FltCond))
	sb.WriteString(fmt.Sprintf("\nPartition: %s\n", sd.DestTags.Line()))
	return api.ExecResult{Output: sb.String()}, nil
}

func (ad *Admin) cmdDescribePartition(d *lql.TagsVal) (api.ExecResult, error) {
	pi, err := ad.Partitions.GetParitionInfo(d.Tags.Line().String())
	if err != nil {
		return api.ExecResult{}, err
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\nPartition: %s", pi.Tags.Line().String()))
	sb.WriteString(fmt.Sprintf("\nId:        %s", pi.JournalId))
	sb.WriteString(fmt.Sprintf("\nRecords:   %d", pi.Records))
	sb.WriteString(fmt.Sprintf("\nSize:      %s (%d)", humanize.Bytes(pi.Size), pi.Size))
	sb.WriteString(fmt.Sprintf("\nChunks:    %d\n", len(pi.Chunks)))
	for i, ci := range pi.Chunks {
		sb.WriteString(fmt.Sprintf("\nChunk #%d:", i+1))
		sb.WriteString(fmt.Sprintf("\n\tId:        %s", ci.Id.String()))
		sb.WriteString(fmt.Sprintf("\n\tRecords:   %d", ci.Records))
		sb.WriteString(fmt.Sprintf("\n\tSize:      %s (%d)", humanize.Bytes(uint64(ci.Size)), ci.Size))
		sb.WriteString(fmt.Sprintf("\n\tT1(small): %s", time.Unix(0, int64(ci.MinTs)).String()))
		sb.WriteString(fmt.Sprintf("\n\tT2(big):   %s\n", time.Unix(0, int64(ci.MaxTs)).String()))
		sb.WriteString(fmt.Sprintf("\n\tComment:   %s\n", ci.Comment))
	}

	return api.ExecResult{Output: sb.String()}, nil
}
