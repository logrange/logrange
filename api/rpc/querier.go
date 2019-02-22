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
	"github.com/logrange/logrange/api"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
)

type (
	ServerQuerier struct {
	}

	clntQuerier struct {
		rc rrpc.Client
	}
)

func (cq *clntQuerier) Query(lql string, res *api.QueryResult) error {
	resp, opErr, err := cq.rc.Call(context.Background(), cRpcEpQuerierQuery, xbinary.WritableString(lql))
	if err != nil {
		return err
	}

	if res != nil {
		if opErr == nil {
			unmarshalQueryResult(resp, res)
		}
		res.Err = opErr
	}
	cq.rc.Collect(resp)

	return nil
}

func unmarshalQueryResult(buf []byte, res *api.QueryResult) {

}

func (sq *ServerQuerier) query(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	_, _, err := xbinary.UnmarshalString(reqBody, false)
	if err != nil {

	}
}
