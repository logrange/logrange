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

package rpc

import (
	"context"
	"encoding/json"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/records"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
)

type (
	ServerQuerier struct {
		CurProvider *cursor.Provider `inject:""`
		MainCtx     context.Context  `injext:"mainCtx"`
		Pool        *bytes.Pool      `inject:""`
		TIndex      tindex.Service   `inject:""`

		logger log4g.Logger
	}

	clntQuerier struct {
		rc      rrpc.Client
		clsdCtx context.Context
	}

	writableQueryRequest api.QueryRequest
)

func (wqr *writableQueryRequest) WritableSize() int {
	return getQueryRequestSize((*api.QueryRequest)(wqr))
}

func (wqr *writableQueryRequest) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	return writeQueryRequest((*api.QueryRequest)(wqr), ow)
}

func (cq *clntQuerier) Query(req *api.QueryRequest, res *api.QueryResult) error {
	resp, opErr, err := cq.rc.Call(cq.clsdCtx, cRpcEpQuerierQuery, (*writableQueryRequest)(req))
	if err != nil {
		return err
	}

	if res != nil {
		if opErr == nil {
			unmarshalQueryResult(resp, res, true)
		}
		res.Err = opErr
	}
	cq.rc.Collect(resp)

	return nil
}

func (cq *clntQuerier) Sources(TagsCond string, res *api.SourcesResult) error {
	resp, opErr, err := cq.rc.Call(cq.clsdCtx, cRpcEpQuerierSources, xbinary.WritableString(TagsCond))
	if err != nil {
		return err
	}

	if res != nil {
		if opErr == nil {
			err = json.Unmarshal(resp, res)
		}
		res.Err = opErr
	}
	cq.rc.Collect(resp)

	return err
}

func NewServerQuerier() *ServerQuerier {
	sq := new(ServerQuerier)
	sq.logger = log4g.GetLogger("rpc.querier")
	return sq
}

func (sq *ServerQuerier) query(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	var rq api.QueryRequest
	_, err := unmarshalQueryRequest(reqBody, &rq, false)
	if err != nil {
		sq.logger.Warn("query(): receive a request with unmarshalable body err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	state := cursor.State{Id: rq.ReqId, Where: rq.Where, Sources: rq.TagsCond, Pos: rq.Pos}
	cur, err := sq.CurProvider.GetOrCreate(sq.MainCtx, state)
	if err != nil {
		sq.logger.Warn("query(): Could not get/create a cursor, err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	limit := rq.Limit
	if limit < 0 || limit > 10000 {
		limit = 10000
	}
	lim := limit

	var qr queryResultBuilder
	var le api.LogEvent
	var lge model.LogEvent
	var tags model.TagLine

	qr.init(sq.Pool)
	for limit > 0 && err == nil {
		lge, tags, err = cur.Get(sq.MainCtx)
		if err == nil {
			le.Tags = string(tags)
			le.Message = lge.Msg
			le.Timestamp = lge.Timestamp
			qr.writeLogEvent(&le)
		}
		cur.Next(sq.MainCtx)
	}

	state = sq.CurProvider.Release(sq.MainCtx, cur)
	if err == nil || err == io.EOF {
		qryReq := &api.QueryRequest{ReqId: state.Id, TagsCond: state.Sources, Where: state.Where, Pos: state.Pos, Limit: lim}
		err = qr.writeQueryRequest(qryReq)
		if err == nil {
			sc.SendResponse(reqId, nil, records.Record(qr.buf()))
		}
	}

	if err != nil {
		sc.SendResponse(reqId, err, cEmptyResponse)
	}
	qr.Close()
}

func (sq *ServerQuerier) sources(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	_, tagsCond, err := xbinary.UnmarshalString(reqBody, false)
	if err != nil {
		sq.logger.Warn("sources(): receive a request with unmarshalable body err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	se, err := lql.ParseExpr(tagsCond)
	if err != nil {
		sq.logger.Warn("sources(): could not parse ", tagsCond, " body err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	mp, count, err := sq.TIndex.GetJournals(se, 100, true)
	if err != nil {
		sq.logger.Warn("sources(): could not obtain sources err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	srcs := make([]api.Source, len(mp))
	idx := 0
	for tags := range mp {
		srcs[idx].Tags = string(tags)
		idx++
	}

	resp := api.SourcesResult{Sources: srcs, Count: count}
	buf, err := json.Marshal(resp)
	if err != nil {
		sq.logger.Warn("sources(): could not marshal response err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}
	sc.SendResponse(reqId, nil, records.Record(buf))
}
