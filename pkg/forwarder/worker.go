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

package forwarder

import (
	"context"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/forwarder/sink"
	"github.com/logrange/logrange/pkg/utils"
	"time"
)

type (
	workerConfig struct {
		desc   *desc
		sink   sink.Sink
		rpcc   api.Client
		logger log4g.Logger
	}

	worker struct {
		desc *desc
		rpcc api.Client
		sink sink.Sink

		stop    bool
		stopped bool
		logger  log4g.Logger
	}
)

//===================== worker =====================

func newWorker(wc *workerConfig) *worker {
	w := new(worker)
	w.desc = wc.desc
	w.rpcc = wc.rpcc
	w.sink = wc.sink
	w.logger = wc.logger
	w.logger.Info("New for desc=", w.desc)
	return w
}

func (w *worker) run(ctx context.Context) error {
	st, err := w.getStream(ctx)
	if err != nil {
		return err
	}
	qr, err := w.prepareQuery(st.Destination)
	if err != nil {
		return err
	}

	totalCnt := uint64(0)
	sleepDur := 5 * time.Second
	nextStat := time.Now()

	limit := qr.Limit
	timeout := qr.WaitTimeout
	for ctx.Err() == nil && !w.stop {
		qr.Limit = limit
		qr.WaitTimeout = timeout

		if time.Now().After(nextStat) {
			w.logger.Info("Stats (every 10 sec): forwarded ", totalCnt, " events (total), position=", qr.Pos)
			nextStat = time.Now().Add(10 * time.Second)
		}

		res := &api.QueryResult{}
		err = w.rpcc.Query(ctx, qr, res)
		if err != nil {
			w.logger.Error("Failed to execute query=", qr, ", will retry in 5 sec, err=", err)
			utils.Sleep(ctx, sleepDur)
			continue
		}

		if len(res.Events) == 0 {
			w.logger.Info("No new events, sleep 5 sec...")
			utils.Sleep(ctx, sleepDur)
			continue
		}

		err = w.sink.OnEvent(res.Events)
		if err != nil {
			w.logger.Warn("Failed to sink events, will retry in 5 sec, err=", err)
			utils.Sleep(ctx, sleepDur)
			continue
		}

		qr = &res.NextQueryRequest
		w.desc.setPosition(qr.Pos)
		totalCnt += uint64(len(res.Events))
	}

	_ = w.sink.Close()
	w.stopped = true
	w.logger.Warn("Stopped; pos=", qr.Pos, ", err=", err)
	return nil
}
func (w *worker) stopGracefully() {
	if !w.stopped && !w.stop {
		w.logger.Info("Stopping...")
		w.stop = true
	}
}
func (w *worker) isStopped() bool {
	return w.stopped
}

func (w *worker) getStream(ctx context.Context) (api.Pipe, error) {
	st := api.Pipe{
		Name:       w.desc.Worker.Name,
		TagsCond:   w.desc.Worker.Stream.Source,
		FilterCond: w.desc.Worker.Stream.Filter,
	}

	res := &api.PipeCreateResult{}
	err := w.rpcc.EnsurePipe(ctx, st, res)
	if err != nil {
		return api.Pipe{}, err
	}

	return res.Pipe, res.Err
}

func (w *worker) prepareQuery(dest string) (*api.QueryRequest, error) {
	qr := &api.QueryRequest{
		Query:       fmt.Sprintf("select source %v", dest),
		Pos:         w.desc.getPosition(),
		Limit:       1000,
		WaitTimeout: 10,
	}
	return qr, nil
}
