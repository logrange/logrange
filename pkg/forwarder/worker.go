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
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/forwarder/sink"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/utils"
	"math"
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
	qr, err := w.prepareQuery()
	if err == nil {
		totalCnt := uint64(0)
		sleepDur := 5 * time.Second
		nextStat := time.Now()

		limit := qr.Limit
		timeout := qr.WaitTimeout
		for ctx.Err() == nil && !w.stop {
			qr.Limit = limit
			qr.WaitTimeout = timeout

			if time.Now().After(nextStat) {
				w.logger.Info("Forwarded in total ", totalCnt, " events!")
				nextStat = time.Now().Add(10 * time.Second)
			}

			res := &api.QueryResult{}
			err := w.rpcc.Query(ctx, qr, res)
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
	}

	_ = w.sink.Close()
	w.stopped = true
	w.logger.Warn("Stopped, err=", err)
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

func (w *worker) prepareQuery() (*api.QueryRequest, error) {
	l, err := lql.ParseLql(w.desc.Worker.Source.Lql)
	if err != nil {
		return nil, err
	}
	s := l.Select

	pos := w.desc.getPosition()
	if s.Position != nil {
		pos = s.Position.PosId
	}

	lim := utils.GetInt64Val(s.Limit, 0)
	if lim > math.MaxInt32 {
		lim = math.MaxInt32
	}

	qr := &api.QueryRequest{
		Query:       w.desc.Worker.Source.Lql,
		Pos:         pos,
		Limit:       int(lim),
		WaitTimeout: 10,
	}
	return qr, nil
}
