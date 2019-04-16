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
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/logrange/logrange/pkg/scanner/parser"
	"github.com/logrange/logrange/pkg/utils"
	"io"
	"sync/atomic"
	"time"
)

type (
	workerConfig struct {
		desc         *desc
		schema       *schema
		recsPerEvent int
		parser       parser.Parser
		logger       log4g.Logger
	}

	worker struct {
		desc         *desc
		meta         model.Meta
		recsPerEvent int

		state  int32
		confCh chan struct{}

		parser parser.Parser
		logger log4g.Logger
	}

	stats struct {
		Id          string
		Filename    string
		ParserStats *parser.Stats
	}
)

const (
	wsRunning = int32(iota)
	wsRunUntilEof
	wsStopped
)

//===================== worker =====================

func newWorker(wc *workerConfig) *worker {
	sm := wc.schema.getMeta(wc.desc)
	w := new(worker)
	w.desc = wc.desc
	w.parser = wc.parser
	w.meta = model.Meta{Tags: sm.Tags, Fields: sm.Fields}
	w.recsPerEvent = wc.recsPerEvent
	w.logger = wc.logger
	w.state = wsRunning
	w.logger.Info("New for desc=", w.desc)
	return w
}

func (w *worker) run(ctx context.Context, events chan<- *model.Event) error {
	err := w.parser.SetStreamPos(w.desc.getOffset())
	if err == nil {
		w.confCh = make(chan struct{})
		defer close(w.confCh)

		recs := make([]*model.Record, 0, w.recsPerEvent)
		for ctx.Err() == nil && err == nil {
			var rec *model.Record
			rec, err = w.parser.NextRecord(ctx)
			if rec != nil {
				recs = append(recs, rec)
			}

			eof := err == io.EOF
			if eof || len(recs) == w.recsPerEvent {
				err = w.sendOrSleep(ctx, recs, events)
				recs = w.recycle(recs)
			}

			if eof && atomic.LoadInt32(&w.state) == wsRunUntilEof && err == nil {
				w.logger.Info("EOF reached!")
				err = io.EOF
			}
		}
	}

	_ = w.parser.Close()
	atomic.StoreInt32(&w.state, wsStopped)
	w.logger.Warn("Stopped, err=", err)
	return err
}

func (w *worker) stopOnEOF() {
	if atomic.CompareAndSwapInt32(&w.state, wsRunning, wsRunUntilEof) {
		w.logger.Info("Stopping on EOF...")
	}
}

func (w *worker) isStopped() bool {
	return atomic.LoadInt32(&w.state) == wsStopped
}

func (w *worker) recycle(recs []*model.Record) []*model.Record {
	for i := 0; i < len(recs); i++ {
		recs[i] = nil
	}
	return recs[:0]
}

func (w *worker) sendOrSleep(ctx context.Context, recs []*model.Record, events chan<- *model.Event) error {
	if len(recs) == 0 {
		utils.Sleep(ctx, time.Second)
		return nil
	}

	ev := model.NewEvent(w.desc.File, recs, w.meta, w.confCh)

	select {
	case <-ctx.Done():
		return fmt.Errorf("interrupted")
	case events <- ev:
	}

	w.waitConfirm(ctx)
	return nil
}

func (w *worker) waitConfirm(ctx context.Context) {
	select {
	case <-ctx.Done():
	case w.confCh <- struct{}{}:
		w.desc.setOffset(w.parser.GetStreamPos())
	}
}

func (w *worker) GetStats() *stats {
	ps := w.parser.GetStats()
	ps.FileStats.Pos = w.desc.getOffset()
	return &stats{Filename: w.desc.File, Id: w.desc.Id, ParserStats: ps}
}
