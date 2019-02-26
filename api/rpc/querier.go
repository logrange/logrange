// Copyright 2018 The logrange Authors
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
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/model"
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

		logger log4g.Logger
	}

	clntQuerier struct {
		rc rrpc.Client
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
	resp, opErr, err := cq.rc.Call(context.Background(), cRpcEpQuerierQuery, (*writableQueryRequest)(req))
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

	state := cursor.State{Id: rq.ReqId, Where: rq.Where, Sources: rq.Tags, Pos: rq.Pos}
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
		qryReq := &api.QueryRequest{ReqId: state.Id, Tags: state.Sources, Where: state.Where, Pos: state.Pos, Limit: lim}
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
