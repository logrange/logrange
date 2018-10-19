package scanner

import (
	"context"
	"github.com/logrange/logrange/pkg/collector/model"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrivets/log4g"
)

type (
	// workerConfig is a structure which is used to provide init parameters for a
	// a new worker
	workerConfig struct {
		desc   *desc
		parser Parser
		logger log4g.Logger
		ctx    context.Context
		cancel context.CancelFunc

		// EventMaxRecCount defines maximum number of records per event
		EventMaxRecCount int
	}

	worker struct {
		logger      log4g.Logger
		desc        *desc
		maxRecCount int
		ctx         context.Context
		ctxCancel   context.CancelFunc
		recParser   Parser
		lock        sync.Mutex
		confCh      chan struct{}
		existChck   time.Time
		state       int32
	}

	WorkerStats struct {
		Filename    string
		Id          string
		ParserStats *ParserStats
	}

	onEventFunc func(ev *model.Event) error
)

const (
	wsRunning = int32(iota)
	wsRunningUntilEof
	wsClosed
)

func newWorker(wc *workerConfig) *worker {
	w := new(worker)
	w.logger = wc.logger
	w.desc = wc.desc
	w.ctx, w.ctxCancel = wc.ctx, wc.cancel
	w.recParser = wc.parser
	w.maxRecCount = wc.EventMaxRecCount
	w.state = wsRunning
	w.logger.Info("New worker for ", w.desc)
	return w
}

func (w *worker) run(oef onEventFunc) (err error) {
	if err = w.recParser.SetStreamPos(w.desc.getOffset()); err != nil {
		return err
	}
	w.confCh = make(chan struct{})
	defer close(w.confCh)

	recs := make([]*model.Record, 0, w.maxRecCount)
	for w.ctx.Err() == nil && err == nil {
		var rec *model.Record
		rec, err = w.recParser.NextRecord()
		if rec != nil {
			recs = append(recs, rec)
		}

		if err == io.EOF || len(recs) == w.maxRecCount {
			eof := err == io.EOF
			err = w.sendRecs(recs, oef, w.recParser.GetStreamPos())
			recs = recs[:0]
			if eof && atomic.LoadInt32(&w.state) == wsRunningUntilEof && err == nil {
				// break the cycle
				w.logger.Info("EOF is reached in wsRunningUntilEof state")
				err = io.EOF
			}
		}
	}
	w.logger.Info("Over handling records err=", err)
	return err
}

func (w *worker) sendRecs(recs []*model.Record, oef onEventFunc, offs int64) error {
	if len(recs) == 0 {
		select {
		case <-w.ctx.Done():
		case <-time.After(time.Second):
		}
		return nil
	}

	defer w.clear(recs)
	ev := model.NewEvent(w.desc.File, recs, w.confCh)
	err := oef(ev)
	if err != nil {
		return err
	}

	select {
	case <-w.ctx.Done():
	case w.confCh <- struct{}{}:
		w.desc.setOffset(offs)
	}
	return nil
}

func (w *worker) clear(recs []*model.Record) {
	for i := 0; i < len(recs); i++ {
		recs[i] = nil
	}
}

func (w *worker) stopWhenEof() {
	w.logger.Info("Set wsRunningUntilEof")
	atomic.CompareAndSwapInt32(&w.state, wsRunning, wsRunningUntilEof)
}

func (w *worker) isClosed() bool {
	return atomic.LoadInt32(&w.state) == wsClosed
}

func (w *worker) Close() (err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	atomic.StoreInt32(&w.state, wsClosed)
	w.ctxCancel()
	if w.recParser != nil {
		err = w.recParser.Close()
	}
	return
}

func (w *worker) GetStats() *WorkerStats {
	ps := w.recParser.GetStats()
	ps.Pos = w.desc.getOffset()
	return &WorkerStats{Filename: w.desc.File, Id: w.desc.Id, ParserStats: ps}
}
