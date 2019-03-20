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

package scanner

import (
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/api/rpc"
	"github.com/logrange/logrange/client/forwarder"
	"github.com/logrange/logrange/client/forwarder/sink"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/range/pkg/transport"
	"github.com/logrange/range/pkg/utils/bytes"
	"os"
	"time"
)

type (
	Scanner interface {
		Run(context.Context) error
	}

	rpcScnanner struct {
		//cfg     forwarder.ScanConfig
		trCfg   transport.Config
		storage storage.Storage
		client  *rpc.Client
		logger  log4g.Logger
		sink    sink.Sink
	}
)

const (
	cRpcScanDefaultLimit       = 1000
	cRpcScanDefaultWaitTimeout = 10
	cRpcStorageKey             = "lr-fwd"
)

//===================== rpcScanner =====================

func NewRpcScanner(cfg *forwarder.Config, storage storage.Storage, sink sink.Sink) Scanner {
	return &rpcScnanner{
		logger: log4g.GetLogger("lr-fwd"),
		//cfg:     cfg.ScanConfig,
		trCfg:   cfg.Transport,
		storage: storage,
		sink:    sink,
	}
}

func (rs *rpcScnanner) connect() error {
	if rs.client != nil {
		return nil
	}
	c, err := rpc.NewClient(rs.trCfg)
	if err != nil {
		return err
	}
	rs.client = c
	return err
}

func (rs *rpcScnanner) disconnect() {
	if rs.client != nil {
		rs.client.Close()
		rs.client = nil
	}
}

func (rs *rpcScnanner) Run(ctx context.Context) error {
	req, err := rs.prepareFirstQuery()
	if err != nil {
		rs.logger.Error("Run(): Could not create first request err=", err, ". Aborting.")
		return err
	}

	rs.logger.Info("Run() starting with the request: ", &req)

	var qr api.QueryResult
	for ctx.Err() == nil {
		err := rs.connect()
		if err != nil {
			rs.disconnect()
			sleepDur := time.Second
			rs.logger.Error("Connection error ", err, " will reconnect in ", sleepDur)
			utils.Sleep(ctx, sleepDur)
			continue
		}

		//err = rs.client.Querier().Query(ctx, &req, &qr)
		if err != nil {
			rs.disconnect()
			sleepDur := time.Second
			rs.logger.Error("Could not make server call query. err=", err,
				" will try again in ", sleepDur)
			utils.Sleep(ctx, sleepDur)
			continue
		}

		if qr.Err != nil {
			sleepDur := time.Second
			rs.logger.Warn("got query execution error=", qr.Err,
				", will try the request once again in ", sleepDur)
			utils.Sleep(ctx, sleepDur)
			continue
		}

		req.ReqId = qr.NextQueryRequest.ReqId
		req.Pos = qr.NextQueryRequest.Pos
		if rs.sinking(ctx, qr.Events) {
			rs.savePos(req.Pos)
		}
	}
	return ctx.Err()
}

func (rs *rpcScnanner) sinking(ctx context.Context, events []*api.LogEvent) bool {
	for ctx.Err() == nil {
		err := rs.sink.OnNewData(events)
		if err == nil {
			return true
		}
		sleepDur := time.Second
		rs.logger.Warn("could not sink portion of ", len(events), " err=", err, ", will try again in ", sleepDur)
		utils.Sleep(ctx, sleepDur)
	}
	return false
}

func (rs *rpcScnanner) prepareFirstQuery() (api.QueryRequest, error) {
	var res api.QueryRequest
	_, err := lql.ParseLql("") //(rs.cfg.Lql)
	if err != nil {
		rs.logger.Error("Could not parse ")
		return res, err
	}

	res.Limit = cRpcScanDefaultLimit
	res.WaitTimeout = cRpcScanDefaultWaitTimeout
	//res.Query = rs.cfg.Lql
	res.Pos, err = rs.readPos()
	return res, err
}

func (rs *rpcScnanner) savePos(pos string) error {
	return rs.storage.WriteData(cRpcStorageKey, bytes.StringToByteArray(pos))
}

func (rs *rpcScnanner) readPos() (string, error) {
	data, err := rs.storage.ReadData(cRpcStorageKey)
	if err != nil {
		if err == os.ErrNotExist {
			rs.logger.Info("no position saved by key=", cRpcStorageKey, " will report empty")
			err = nil
		}

		return "", err
	}
	return bytes.ByteArrayToString(data), nil
}
