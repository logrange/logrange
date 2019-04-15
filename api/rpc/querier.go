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
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/backend"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
	"time"
)

type (
	ServerQuerier struct {
		CurProvider cursor.Provider  `inject:""`
		MainCtx     context.Context  `inject:"mainCtx"`
		Pool        *bytes.Pool      `inject:""`
		Querier     *backend.Querier `inject:""`

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

func (cq *clntQuerier) Query(ctx context.Context, req *api.QueryRequest, res *api.QueryResult) error {
	resp, opErr, err := cq.rc.Call(ctx, cRpcEpQuerierQuery, (*writableQueryRequest)(req))
	if err != nil {
		return err
	}

	if res != nil {
		if opErr == nil {
			_, err = unmarshalQueryResult(resp, res, true)
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

// query is the server version of api.Querier.Query(). It is optimized to produce high performance records read
// for general API purpoces backend.Querier could be used instead
func (sq *ServerQuerier) query(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	var rq api.QueryRequest
	_, err := unmarshalQueryRequest(reqBody, &rq, false)
	if err != nil {
		sq.logger.Warn("query(): receive a request with unmarshalable body err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	if rq.WaitTimeout < 0 || rq.WaitTimeout > backend.QueryMaxWaitTimeout {
		sc.SendResponse(reqId, fmt.Errorf("wrong wait timeout. Must be in range [0..%d], but provided %d",
			backend.QueryMaxWaitTimeout, rq.WaitTimeout), cEmptyResponse)
		return
	}

	limit := rq.Limit
	if limit < 0 {
		sc.SendResponse(reqId, fmt.Errorf("limit is negative"), cEmptyResponse)
		return
	}

	if limit > backend.QueryMaxLimit {
		limit = backend.QueryMaxLimit
	}
	lim := limit

	cache := rq.WaitTimeout > 0 || limit != rq.Limit

	if lim == 0 && rq.WaitTimeout <= 0 {
		sq.logger.Debug("query(): returns empty response for limit=0 and not blocking query")
		sc.SendResponse(reqId, nil, cEmptyResponse)
		return
	}

	state := cursor.State{Id: rq.ReqId, Query: rq.Query, Pos: rq.Pos}
	cur, err := sq.CurProvider.GetOrCreate(sq.MainCtx, state, cache)
	if err != nil {
		sq.logger.Warn("query(): Could not get/create a cursor, err=", err, " state=", state)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	var qr queryResultBuilder
	var le api.LogEvent
	var lge model.LogEvent
	var tags tag.Line

	qr.init(sq.Pool)
	flds := field.Fields("")
	kvsFields := ""

	for limit > 0 && err == nil {
		lge, tags, err = cur.Get(sq.MainCtx)
		if err == nil {
			if lge.Fields != flds {
				kvsFields = lge.Fields.AsKVString()
				flds = lge.Fields.MakeCopy()
			}
			le.Tags = string(tags)
			le.Message = lge.Msg.AsWeakString()
			le.Timestamp = lge.Timestamp
			le.Fields = kvsFields
			qr.writeLogEvent(&le)
			limit--
			cur.Next(sq.MainCtx)
		}

		if err == io.EOF && limit == lim && rq.WaitTimeout > 0 {
			ctx, _ := context.WithTimeout(sq.MainCtx, time.Duration(rq.WaitTimeout)*time.Second)
			err = cur.WaitNewData(ctx)
			if err != nil {
				sq.logger.Debug("Waited for new data for ", rq.WaitTimeout, " seconds and expired. cur=", cur)
				err = nil
				break
			}
		}
	}

	state = sq.CurProvider.Release(sq.MainCtx, cur)

	if err == nil || err == io.EOF {
		qryReq := &api.QueryRequest{ReqId: state.Id, Query: state.Query, Pos: state.Pos, Limit: lim, WaitTimeout: rq.WaitTimeout}
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
