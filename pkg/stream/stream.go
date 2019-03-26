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
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cursor"
	journal2 "github.com/logrange/logrange/pkg/journal"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records/journal"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"sync"
)

type (
	strm struct {
		svc  *Service
		cfg  Stream
		tags tag.Set
		srcF lql.TagsExpFunc
		fltF lql.WhereExpFunc

		lock    sync.Mutex
		logger  log4g.Logger
		clsCtx  context.Context
		cancelF context.CancelFunc

		// the map contains state of all sources that affected the stream somehow.
		// TODO how we are going to clean it up when a source is removed?
		sources map[string]*ssDesc
	}

	ssDesc struct {
		Tags string
		// Pos contains synced position (reported by worker or by new write)
		Pos journal.Pos
		// LastKnwnPos contains information about a notification regarding last written pos
		LastKnwnPos journal.Pos

		wCharged bool

		curId uint64
	}
)

func newStrm(svc *Service, st Stream) (*strm, error) {
	srcF, err := lql.BuildTagsExpFunc(st.TagsCond)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse source condition %s", st.TagsCond)
	}

	fltF, err := lql.BuildWhereExpFunc(st.FltCond)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse filter condition %s", st.FltCond)
	}

	s := new(strm)
	s.svc = svc
	s.srcF = srcF
	s.fltF = fltF
	s.cfg = st
	s.clsCtx, s.cancelF = context.WithCancel(svc.closedCtx)
	s.tags = streamTags(st.Name)
	s.sources = make(map[string]*ssDesc)
	s.logger = log4g.GetLogger("stream.strm").WithId("{" + st.Name + "}").(log4g.Logger)
	svc.sp.loadStreamInfo(st.Name, s.sources)
	return s, nil
}

func (s *strm) onWriteEvent(we *journal2.WriteEvent) {
	s.lock.Lock()
	sd, ok := s.sources[we.Src]
	if !ok {
		s.logger.Debug("onWriteEvent(): first notification about a change ", we)
		sd = new(ssDesc)
		sd.Pos = we.StartPos
		sd.Tags = we.Tags.Line().String()
		s.sources[we.Src] = sd
	}
	sd.LastKnwnPos = we.EndPos
	s.startWorker(we.Src, we.Tags.Line(), sd)
	s.lock.Unlock()
}

func (s *strm) startWorker(src string, srcTags tag.Line, sd *ssDesc) {
	s.svc.wwg.Add(1)
	if s.svc.closedCtx.Err() == nil && !sd.wCharged && sd.Pos.Less(sd.LastKnwnPos) {
		sd.wCharged = true
		w := newWorker(s, src, srcTags, s.tags.Line())
		go w.run(s.clsCtx)
	} else {
		s.svc.wwg.Done()
	}
}

// getState returns the stream state
func (s *strm) getState(src string) (cursor.State, error) {
	s.lock.Lock()
	sd, ok := s.sources[src]
	if !ok {
		s.lock.Unlock()
		return cursor.State{}, errors2.NotFound
	}
	res := cursor.State{Id: sd.curId, Query: "select source {" + sd.Tags + "}", Pos: src + "=" + sd.Pos.String()}
	s.lock.Unlock()
	return res, nil
}

// delete is the signal to delete the stream and all metadata associated with it.
func (s *strm) delete() {
	s.cancelF()
	s.svc.sp.onDeleteStream(s.cfg.Name)
}

func (s *strm) saveState(src string, st cursor.State) error {
	s.lock.Lock()
	sd, ok := s.sources[src]
	if !ok {
		s.lock.Unlock()
		s.logger.Warn("Saving state for src=", src, ", which is not found. Ignoring. state=", st)
		return errors2.NotFound
	}

	pos, err := s.curPos2JrnlPos(src, st.Pos)
	if err == nil {
		sd.Pos = pos
		sd.curId = st.Id
		s.svc.sp.saveStreamInfo(s.cfg.Name, s.sources)
	}
	s.lock.Unlock()
	return err
}

func (s *strm) workerDone(w *worker) {
	defer s.svc.wwg.Done()

	s.lock.Lock()
	sd, ok := s.sources[w.src]
	if !ok {
		s.logger.Warn("workerDone(): Could not find the descriptor for src=", w.src, ". Deleted? Leaving as is...")
		s.lock.Unlock()
		return
	}
	sd.wCharged = false
	// to be sure that there is no data while finishing the worker
	s.startWorker(w.src, w.srcTags, sd)
	s.lock.Unlock()
}

// isListeningTheSource returns true if the source tags are matched to the criteria condition
func (s *strm) isListeningTheSource(tags tag.Set) bool {
	return s.srcF(tags)
}

func (s *strm) getConfig() Stream {
	// it is immutable, so no guards here
	return s.cfg
}

func (s *strm) curPos2JrnlPos(src, cpos string) (journal.Pos, error) {
	res := strings.Split(cpos, "=")
	if len(res) != 2 {
		return journal.Pos{}, errors.Errorf("could not parse %s as a journal position ", cpos)
	}

	if res[0] != src {
		return journal.Pos{}, errors.Errorf("expecting src=%s, but the position %s has src=%s", src, cpos, res[0])
	}

	return journal.ParsePos(res[1])
}

// streamTags returns a stream tags by the stream name
func streamTags(name string) tag.Set {
	ts, _ := tag.Parse("logrange.stream=" + strconv.Quote(name))
	return ts
}
