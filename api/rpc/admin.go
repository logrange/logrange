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
	"github.com/logrange/logrange/pkg/backend"
	"github.com/logrange/range/pkg/records"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/pkg/errors"
)

type (
	// ServerAdmin implements server part of api.Admin interface
	ServerAdmin struct {
		Admin *backend.Admin `inject:""`

		logger log4g.Logger
	}

	clntAdmin struct {
		rc rrpc.Client
	}
)

func (ca *clntAdmin) Execute(ctx context.Context, req api.ExecRequest) (res api.ExecResult, err error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return res, errors.Wrapf(err, "could not marshal request ")
	}

	resp, opErr, err := ca.rc.Call(ctx, cRpcEpAdminExecute, records.Record(buf))
	if err != nil {
		return res, errors.Wrapf(err, "could not sent request via rpc")
	}

	if opErr == nil {
		err = json.Unmarshal(resp, &res)
	}
	res.Err = opErr
	ca.rc.Collect(resp)

	return
}

// NewServerAdmin creates a new instance of ServerAdmin
func NewServerAdmin() *ServerAdmin {
	sa := new(ServerAdmin)
	sa.logger = log4g.GetLogger("rpc.admin")
	return sa
}

func (sa *ServerAdmin) execute(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	var req api.ExecRequest
	err := json.Unmarshal(reqBody, &req)
	if err != nil {
		sa.logger.Error("execute(): could not unmarshal the body request ")
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	res, err := sa.Admin.Execute(req)
	if err != nil {
		sa.logger.Warn("execute(): got the err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	buf, err := json.Marshal(res)
	if err != nil {
		sa.logger.Warn("execute(): could not marshal result err=", err)
		sc.SendResponse(reqId, err, cEmptyResponse)
		return
	}

	sc.SendResponse(reqId, nil, records.Record(buf))
}
