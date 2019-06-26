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

package partition

import (
	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/context"
	"github.com/logrange/range/pkg/records/chunk"
	"sync"
)

type (
	// TmIndexRebuilder allows to send requests for rebuilding corrupted time index data
	TmIndexRebuilder interface {
		// RebuildIndex sends request for rebuilding time index. It works asynchronously
		// and should not block the go-routine
		// if force is true the index will be rebuilt even if it is alive
		RebuildIndex(src string, cid chunk.Id, force bool)
	}

	tmirebuilder struct {
		logger   log4g.Logger
		parts    *Service
		lock     sync.Mutex
		sema     chan bool
		closedCh chan struct{}

		chunks map[chunk.Id]rebuildStatus
	}

	rebuildStatus struct {
		src     string
		started bool
		force   bool
	}
)

func newTmirebuilder(parts *Service, maxWorkers int) *tmirebuilder {
	tir := new(tmirebuilder)
	tir.parts = parts
	tir.sema = make(chan bool, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		tir.sema <- true
	}

	tir.chunks = make(map[chunk.Id]rebuildStatus)
	tir.closedCh = make(chan struct{})
	tir.logger = log4g.GetLogger("tmiRebuilder")
	return tir
}

func (tir *tmirebuilder) close() {
	close(tir.closedCh)
}

// RebuildIndex allows to request an index rebuilding. The function never blocks
// the go routine which invokes it, so just put the request into the queue.
func (tir *tmirebuilder) RebuildIndex(src string, cid chunk.Id, force bool) {
	tir.lock.Lock()
	if _, ok := tir.chunks[cid]; ok {
		// already run or scheduled
		tir.lock.Unlock()
		return
	}

	startWorker := false
	select {
	case <-tir.sema:
		startWorker = true
	case <-tir.closedCh:
		//closed
		tir.lock.Unlock()
		return
	default:
		//
		tir.logger.Debug("All workers seems to be busy, adding the chunk ", cid, " into waiting list")
	}
	tir.chunks[cid] = rebuildStatus{src, startWorker, force}
	tir.lock.Unlock()

	if startWorker {
		go tir.runWorker(src, cid, force)
	}
}

func (tir *tmirebuilder) runWorker(src string, cid chunk.Id, force bool) {
	tir.logger.Debug("New worker started for serving partition ", src, ", chunkId=", cid)
	defer tir.doneWorker()

	for cid != 0 {
		tir.serve(src, cid, force)

		tir.lock.Lock()
		delete(tir.chunks, cid)
		cid = 0
		for c, rs := range tir.chunks {
			if !rs.started {
				cid = c
				src = rs.src
				force = rs.force
				rs.started = true
				tir.chunks[cid] = rs
				tir.logger.Debug("picking up ", cid, " for serving from the list")
				break
			}
		}
		tir.lock.Unlock()
	}
}

func (tir *tmirebuilder) doneWorker() {
	tir.sema <- true
	tir.logger.Debug("worker done")
}

func (tir *tmirebuilder) serve(src string, cid chunk.Id, force bool) {
	_, err := tir.parts.TIndex.GetJournalTags(src, true)
	if err != nil {
		tir.logger.Warn("Could not acquire partition ", src, ", the err=", err)
		return
	}
	defer tir.parts.TIndex.Release(src)

	ctx := context.WrapChannel(tir.closedCh)
	jrnl, err := tir.parts.Journals.GetOrCreate(ctx, src)
	if err != nil {
		tir.logger.Warn("could not obtain journal ", src, " err=", err)
		return
	}

	chks, err := jrnl.Chunks().Chunks(ctx)
	if err != nil {
		tir.logger.Warn("could not obtain list of chunks for journal ", src, ", drr=", err)
		return
	}

	idx := chunk.FindChunkById(chks, cid)
	if idx < 0 {
		tir.logger.Warn("could not finc the chunk with id=", cid, " for journal ", src)
		return
	}

	tir.logger.Debug("starting to build the time index for chunk id=", cid, " journal ", src, " force=", force)
	tir.parts.TsIndexer.RebuildIndex(ctx, src, chks[idx], force)
}
