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
	"github.com/logrange/logrange/pkg/stream"
	"github.com/logrange/range/pkg/records"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/pkg/errors"
)

type (
	// ServerAdmin implements server part of api.Admin interface
	ServerStreams struct {
		StrmService *stream.Service `inject:""`

		logger log4g.Logger
	}

	clntStreams struct {
		rc rrpc.Client
	}
)

func (cs *clntStreams) EnsureStream(ctx context.Context, stm api.Stream, res *api.StreamCreateResult) error {
	buf, err := json.Marshal(stm)
	if err != nil {
		return errors.Wrapf(err, "could not marshal request ")
	}

	resp, opErr, err := cs.rc.Call(ctx, cRpcEpStreamsEnsure, records.Record(buf))
	if err != nil {
		return errors.Wrapf(err, "could not sent request via rpc")
	}

	if opErr == nil {
		err = json.Unmarshal(resp, &res)
	}

	res.Err = opErr
	cs.rc.Collect(resp)

	return nil
}

func NewServerStreams() *ServerStreams {
	ss := new(ServerStreams)
	ss.logger = log4g.GetLogger("rpc.streams")
	return ss
}

func (ss *ServerStreams) ensureStream(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	var req api.Stream
	err := json.Unmarshal(reqBody, &req)
	if err != nil {
		ss.logger.Error("ensureStream(): could not unmarshal the body request ")
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	sst := stream.Stream{req.Name, req.TagsCond, req.FilterCond}
	dst, err := ss.StrmService.EnsureStream(sst)
	if err != nil {
		ss.logger.Warn("ensureStream(): got the err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	req.TagsCond = dst.TagsCond
	req.FilterCond = dst.FltCond
	req.Destination = dst.DestTags.Line().String()

	buf, err := json.Marshal(req)
	if err != nil {
		ss.logger.Warn("ensureStream(): could not marshal result err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	sc.SendResponse(reqId, nil, records.Record(buf))
}
