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
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/records/journal"
	"io"
	"strings"
	"time"
)

type (
	// Querier is a backend structure used by an api implementation
	Querier struct {
		TIndex      tindex.Service     `inject:""`
		CurProvider *cursor.Provider   `inject:""`
		Journals    journal.Controller `inject:""`

		logger log4g.Logger
	}
)

const QueryMaxWaitTimeout = 60
const QueryMaxLimit = 10000

func NewQuerier() *Querier {
	q := new(Querier)
	q.logger = log4g.GetLogger("backend.Querier")
	return q
}

// Query allows to run query and receive a result. This is not optimized version, for streaming purposed api.rpc
// version must be used.
func (q *Querier) Query(ctx context.Context, req *api.QueryRequest) (*api.QueryResult, error) {
	if req.WaitTimeout < 0 || req.WaitTimeout > QueryMaxWaitTimeout {
		return nil, fmt.Errorf("wrong wait timeout. Must be in range [0..%d], but provided %d",
			QueryMaxWaitTimeout, req.WaitTimeout)
	}

	limit := req.Limit
	if limit < 0 {
		return nil, fmt.Errorf("wrong limit value, expected not-negative integer, but got %d", limit)
	}

	if limit > QueryMaxLimit {
		q.logger.Debug("Got limit=", limit, ", which is too big, correct it to ", QueryMaxLimit)
		limit = QueryMaxLimit
	}
	lim := limit

	cache := req.WaitTimeout > 0 || limit != req.Limit

	state := cursor.State{Id: req.ReqId, Query: req.Query, Pos: req.Pos}
	cur, err := q.CurProvider.GetOrCreate(ctx, state, cache)
	if err != nil {
		q.logger.Warn("Query(): Could not get/create a cursor, err=", err, " state=", state)
		return nil, err
	}

	var sb strings.Builder
	var lge model.LogEvent
	var tags tag.Line

	events := make([]*api.LogEvent, 0, 100)

	for limit > 0 && err == nil {
		lge, tags, err = cur.Get(ctx)
		if err == nil {
			le := new(api.LogEvent)
			sb.WriteString(lge.Msg)
			le.Tags = string(tags)
			le.Message = sb.String()
			le.Timestamp = lge.Timestamp
			sb.Reset()
			events = append(events, le)
			limit--
			cur.Next(ctx)
		}

		if err == io.EOF && limit == lim && req.WaitTimeout > 0 {
			ctx, _ := context.WithTimeout(ctx, time.Duration(req.WaitTimeout)*time.Second)
			err = cur.WaitNewData(ctx)
			if err != nil {
				q.logger.Debug("Waited for new data for ", req.WaitTimeout, " seconds and expired. cur=", cur)
				err = nil
				break
			}
		}
	}

	state = q.CurProvider.Release(ctx, cur)

	var res *api.QueryResult
	if err == nil || err == io.EOF {
		res = new(api.QueryResult)
		res.NextQueryRequest = api.QueryRequest{ReqId: state.Id, Query: state.Query, Pos: state.Pos, Limit: lim, WaitTimeout: req.WaitTimeout}
		res.Events = events
	}

	return res, err
}

// Sources provides implementation for api.Querier.Source function
func (q *Querier) Sources(ctx context.Context, tagsCond string) (*api.SourcesResult, error) {
	src, err := lql.ParseSource(tagsCond)
	if err != nil {
		q.logger.Warn("Sources(): could not parse the source condition ", tagsCond, " err=", err)
		return nil, err
	}

	srcs := make([]api.Source, 0, 100)
	ts := uint64(0)
	tr := uint64(0)
	count := 0
	var opErr error
	err = q.TIndex.Visit(src, func(tags tag.Set, jrnl string) bool {
		count++
		j, err := q.Journals.GetOrCreate(ctx, jrnl)
		if err != nil {
			q.logger.Error("Could not create of get journal instance for ", jrnl, ", err=", err)
			opErr = err
			return false
		}
		ts += j.Size()
		tr += j.Count()
		if cap(srcs) > len(srcs) {
			srcs = append(srcs, api.Source{Tags: tags.Line().String(), Size: j.Size(), Records: j.Count()})
		}
		return true
	})

	if err != nil {
		q.logger.Warn("Sources(): could not obtain sources err=", err)
		return nil, err
	}

	if opErr != nil {
		q.logger.Warn("Sources(): error in a visitor err=", err, " will report as a failure.")
		return nil, err
	}

	q.logger.Debug("Requested journals for ", tagsCond, ". returned ", len(srcs), " in map with total count=", count)

	return &api.SourcesResult{Sources: srcs, Count: count, TotalSize: ts, TotalRec: tr}, nil
}
