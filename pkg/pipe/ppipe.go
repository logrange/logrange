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

package pipe

import (
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
	journal2 "github.com/logrange/logrange/pkg/partition"
	"github.com/logrange/range/pkg/records/journal"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"sync"
)

type (
	ppipe struct {
		svc  *Service
		cfg  Pipe
		tags tag.Set
		srcF lql.TagsExpFunc
		fltF lql.WhereExpFunc

		lock    sync.Mutex
		logger  log4g.Logger
		clsCtx  context.Context
		cancelF context.CancelFunc
		deleted bool

		// the map contains state of all partitions that affected the pipe somehow.
		partitions map[string]*ppDesc
	}

	ppDesc struct {
		Tags string
		// Pos contains synced position (reported by worker or by new write)
		Pos journal.Pos
		// LastKnwnPos contains information about a notification regarding last written pos
		LastKnwnPos journal.Pos

		wCharged bool

		curId uint64
	}
)

func newPPipe(svc *Service, p Pipe) (*ppipe, error) {
	srcF, err := lql.BuildTagsExpFunc(p.TagsCond)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse source condition %s", p.TagsCond)
	}

	fltF, err := lql.BuildWhereExpFunc(p.FltCond)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse filter condition %s", p.FltCond)
	}

	pp := new(ppipe)
	pp.svc = svc
	pp.srcF = srcF
	pp.fltF = fltF
	pp.cfg = p
	pp.clsCtx, pp.cancelF = context.WithCancel(svc.closedCtx)
	pp.tags = streamTags(p.Name)
	pp.partitions = make(map[string]*ppDesc)
	pp.logger = log4g.GetLogger("pipe.ppipe").WithId("{" + p.Name + "}").(log4g.Logger)
	svc.psr.loadPipeInfo(p.Name, pp.partitions)
	return pp, nil
}

func (pp *ppipe) onWriteEvent(we *journal2.WriteEvent) {
	pp.lock.Lock()
	pd, ok := pp.partitions[we.Src]
	if !ok {
		pp.logger.Debug("onWriteEvent(): first notification about a change ", we)
		pd = new(ppDesc)
		pd.Pos = we.StartPos
		pd.Tags = we.Tags.Line().String()
		pp.partitions[we.Src] = pd
	}
	pd.LastKnwnPos = we.EndPos
	pp.startWorker(we.Src, we.Tags.Line(), pd)
	pp.lock.Unlock()
}

func (pp *ppipe) startWorker(src string, srcTags tag.Line, pd *ppDesc) {
	pp.svc.wwg.Add(1)
	if pp.svc.closedCtx.Err() == nil && !pd.wCharged && pd.Pos.Less(pd.LastKnwnPos) {
		pd.wCharged = true
		w := newWorker(pp, src, srcTags, pp.tags.Line())
		go w.run(pp.clsCtx)
	} else {
		pp.svc.wwg.Done()
	}
}

// getState returns the pipe state
func (pp *ppipe) getState(src string) (cursor.State, error) {
	pp.lock.Lock()
	sd, ok := pp.partitions[src]
	if !ok {
		pp.lock.Unlock()
		return cursor.State{}, errors2.NotFound
	}
	res := cursor.State{Id: sd.curId, Src: src, Pos: src + "=" + sd.Pos.String()}
	pp.lock.Unlock()
	return res, nil
}

// cleanPartitions walks through partitions and removes all that doesn't exist anymore
func (pp *ppipe) cleanPartitions() {
	pp.lock.Lock()
	defer pp.lock.Unlock()

	if pp.deleted {
		return
	}

	removed := 0
	for src := range pp.partitions {
		_, err := pp.svc.Journals.TIndex.GetJournalTags(src, false)
		if err == errors2.NotFound {
			delete(pp.partitions, src)
			removed++
		}
	}

	if removed > 0 {
		pp.logger.Debug("cleanPartitions(): ", removed, " partitions were removed from the pipe settings ")
		pp.svc.psr.savePipeInfo(pp.cfg.Name, pp.partitions)
	}
}

// delete is the signal to delete the pipe and all metadata associated with it.
func (pp *ppipe) delete() {
	pp.lock.Lock()
	pp.deleted = true
	pp.lock.Unlock()
	pp.cancelF()
	pp.svc.psr.onDeleteStream(pp.cfg.Name)
}

func (pp *ppipe) saveState(src string, st cursor.State) error {
	pp.lock.Lock()
	pd, ok := pp.partitions[src]
	if !ok {
		pp.lock.Unlock()
		pp.logger.Warn("Saving state for src=", src, ", which is not found. Ignoring. state=", st)
		return errors2.NotFound
	}

	pos, err := pp.curPos2JrnlPos(src, st.Pos)
	if err == nil {
		pd.Pos = pos
		pd.curId = st.Id
		pp.svc.psr.savePipeInfo(pp.cfg.Name, pp.partitions)
	}
	pp.lock.Unlock()
	return err
}

func (pp *ppipe) workerDone(w *worker) {
	defer pp.svc.wwg.Done()

	pp.lock.Lock()
	pd, ok := pp.partitions[w.src]
	if !ok {
		pp.logger.Warn("workerDone(): Could not find the descriptor for src=", w.src, ". Deleted? Leaving as is...")
		pp.lock.Unlock()
		return
	}
	pd.wCharged = false
	// to be sure that there is no data while finishing the worker
	pp.startWorker(w.src, w.srcTags, pd)
	pp.lock.Unlock()
}

// isListeningThePart returns true if the partition tags are matched to the criteria condition
func (pp *ppipe) isListeningThePart(tags tag.Set) bool {
	return pp.srcF(tags)
}

func (pp *ppipe) getConfig() Pipe {
	// it is immutable, so no guards here
	return pp.cfg
}

func (pp *ppipe) curPos2JrnlPos(src, cpos string) (journal.Pos, error) {
	res := strings.Split(cpos, "=")
	if len(res) != 2 {
		return journal.Pos{}, errors.Errorf("could not parse %s as a partition position ", cpos)
	}

	if res[0] != src {
		return journal.Pos{}, errors.Errorf("expecting src=%s, but the position %s has src=%s", src, cpos, res[0])
	}

	return journal.ParsePos(res[1])
}

// streamTags returns a pipe tags by the pipe name
func streamTags(name string) tag.Set {
	ts, _ := tag.Parse("logrange.pipe=" + strconv.Quote(name))
	return ts
}
