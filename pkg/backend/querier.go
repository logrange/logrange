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
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/partition"
	"io"
	"strings"
	"time"
)

type (
	// Querier is a backend structure used by an api implementation
	Querier struct {
		Journals    *partition.Service `inject:""`
		CurProvider cursor.Provider    `inject:""`

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
	flds := field.Fields("")
	kvsFields := ""

	events := make([]*api.LogEvent, 0, 100)

	for limit > 0 && err == nil {
		lge, tags, err = cur.Get(ctx)
		if err == nil {
			if flds != lge.Fields {
				kvsFields = lge.Fields.AsKVString()
				flds = lge.Fields.MakeCopy()
			}
			le := new(api.LogEvent)
			sb.WriteString(lge.Msg.AsWeakString())
			le.Tags = string(tags)
			le.Message = sb.String()
			le.Timestamp = lge.Timestamp
			le.Fields = kvsFields
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
