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

package stream

import (
	"context"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/model/tag"
	context2 "github.com/logrange/range/pkg/context"
	"io"
	"time"
)

type (
	worker struct {
		strm    *strm
		src     string
		srcTags tag.Line
		dstTags tag.Line
		logger  log4g.Logger
	}
)

func newWorker(strm *strm, src string, srcTags, dstTags tag.Line) *worker {
	w := new(worker)
	w.strm = strm
	w.src = src
	w.srcTags = srcTags
	w.dstTags = dstTags
	w.logger = log4g.GetLogger("stream.wrkr").WithId(fmt.Sprintf("{%s -> %s}", src, strm.cfg.Name)).(log4g.Logger)
	return w
}

func (w *worker) run(ctx context.Context) {
	defer w.strm.workerDone(w)
	w.logger.Debug("entering run()")

	state, err := w.strm.getState(w.src)
	if err != nil {
		w.logger.Error("Could not read state for src=", w.src, " tags=", w.srcTags, ". game is over.")
		return
	}

	cur, err := w.strm.svc.CurProvider.GetOrCreate(ctx, state, true)
	if err != nil {
		w.logger.Warn("Could not obtain cursor by state=", state, ", err=", err)
		return
	}
	defer w.strm.svc.CurProvider.Release(ctx, cur)

	w.logger.Debug("Sync for srcTags=", w.srcTags, " to dstTags=", w.dstTags, ", state=", state)

	flds := field.Parse(w.srcTags.String())
	var si siterator
	si.init(flds, cur)
	werrs := 1

	for ctx.Err() == nil {
		err = w.strm.svc.Journals.Write(ctx, w.dstTags.String(), &si, true)
		if err != nil {
			werrs *= 2
			if werrs > 60 {
				werrs = 60
			}
			w.logger.Warn("could not write data into the partition ", w.dstTags.String(), " will sleep for ", werrs, " second(s) and try again, err=", err)
			context2.Sleep(ctx, time.Duration(werrs)*time.Second)
			continue
		}
		werrs = 1

		if err = w.strm.saveState(w.src, cur.State(ctx)); err != nil {
			w.logger.Error("Could not save the existing state, may be deleted?")
			break
		}

		if _, _, err = si.Get(ctx); err == io.EOF {
			w.logger.Trace("No data in ", w.srcTags, " will sleep up to 10 seconds...")
			ctxWait, _ := context.WithTimeout(ctx, 10*time.Second)
			err = cur.WaitNewData(ctxWait)
			if err != nil {
				w.logger.Debug("Time is out or context is closed")
				break
			}
			w.logger.Trace("awaken")
		}
	}

	w.logger.Debug("Seems no data in ", w.srcTags, ", or the context is closed ctx.Err()=", ctx.Err(), " leaving the worker routine")
}
