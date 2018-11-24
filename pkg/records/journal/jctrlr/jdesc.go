// Copyright 2018 The logrange Authors
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

package jctrlr

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/records/journal"
	"github.com/logrange/logrange/pkg/util"
)

type (
	jDescCtrlr interface {
		newChunkById(jd *jDesc, cid chunk.Id) (chunk.Chunk, error)
		newChunk(jd *jDesc) (chunk.Chunk, error)
		newJournal(jd *jDesc) journal.Journal
	}

	jDesc struct {
		logger log4g.Logger
		lock   sync.Mutex
		// name is the journal name
		name string
		// dir is the directory of the journal
		dir string
		// known the journal chunks
		cDescs map[chunk.Id]*cDesc
		// current state
		state int

		jdc jDescCtrlr
		// sorted list of chunks
		chunks []chunk.Chunk
		jrnl   journal.Journal

		crtCh chan bool
	}
)

const (
	jdStateInit     = 0
	jdStateStarting = 1
	jdStateStarted  = 2
	jdStateClosed   = 3
)

func newJDesc(jdc jDescCtrlr, sj scJournal) *jDesc {
	jd := new(jDesc)
	jd.logger = log4g.GetLogger("jdesc." + sj.name)
	jd.name = sj.name
	jd.dir = sj.dir
	jd.chunks.Store(emptyChunks)
	jd.cDescs = make(map[chunk.Id]*cDesc)
	jd.state = jdStateInit
	jd.jdc = jdc
	jd.applyChunkIds(sj.chunks)
	return jd
}

// ---------------------------- ChnksController ------------------------------
func (jd *jDesc) JournalName() string {
	return jd.name
}

func (jd *jDesc) NewChunk() (chunk.Chunks, error) {
	c, err := jd.jdc.newChunk(jd)
	if err != nil {
		return nil, err
	}
	jd.logger.Debug("NewChunk: ", c)

	jd.lock.Lock()
	if jd.state != jdStateStarted && jd.state != jdStateStarting {
		jd.lock.Unlock()
		c.Close()
		return nil, util.ErrWrongState
	}
	cd := newCDesc(c.Id())
	cd.chunk = new(chunkWrapper)
	cd.chunk.chunk = c
	jd.AddListener(cd)
	jd.rebuildChunksList()
	res := jd.chunks
	jd.lock.Unlock()
	return res, nil
}

func (jd *jDesc) Chunks(ctx context.Context) (chunk.Chunks, error) {
	for {
		var ch chan bool
		jd.lock.Lock()
		if jd.state == jdStateStarted {
			res := jd.chunks
			jd.lock.Unlock()
			return res, nil
		}

		if jd.state != jdStateStarting {
			jd.lock.Unlock()
			return nil, util.ErrWrongState
		}

		cj = jd.crtCh
		jd.lock.Unlock()

		if ctx == nil {
			ctx = context.Background()
		}

		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// ------------------------------- io.Closer ---------------------------------
func (jd *jDesc) Close() error {
	jd.logger.Debug("Close: ", jd)
	jd.lock.Lock()
	if jd.state == jdStateClosed {
		jd.lock.Unlock()
		return util.ErrWrongState
	}

	if jd.state == jdStateStarting {
		close(jd.crtCh)
	}

	jd.state = jdStateClosed
	cDescs := jd.cDescs
	jrnl := jd.jrnl
	jd.cDescs = nil
	jd.chunks = nil
	jd.lock.Unlock()

	go func() {
		for _, cd := range cDescs {
			cd.Close()
		}

		if jrnl != nil {
			clsr, ok := jrnl.(io.Closer)
			if ok {
				clsr.Close()
			}
		}
	}()
}

func (jd *jDesc) stateName() string {
	switch jd.state {
	case jdStateInit:
		return "INIT"
	case jdStateStarted:
		return "STARTED"
	case jdStateStarting:
		return "STARTING"
	case jdStateClosed:
		return "CLOSED"
	}
	return "N/A"
}

func (jd *jDesc) String() string {
	return fmt.Sprintf("{name=%s, state=%s, chunks=%d, dir=%s}", jd.name, jd.stateName(), len(jd.cDescs), jd.dir)
}

func (jd *jDesc) getJournal(ctx context.Context) (journal.Journal, error) {
	done := false
	var err error
	var jrnl journal.Journal

	for !done {
		switch jd.state {
		case jdStateInit:
			jd.setStarting()
		case jdStateStarted:
			jrnl = jd.jrnl
			done = true
		case jdStateClosed:
			err = util.ErrWrongState
			done = true
		}
		ch := jd.crtCh
		jd.lock.Unlock()

		if !done {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				done = true
			case <-ch:
				jd.lock.Lock()
				done = jd.state == jdStateStarted
				jrnl = jd.jrnl
				jd.lock.Unlock()
			}
		}
	}
	return jrnl, err
}

// applyChunkIds is called when some chunks are found on the folder, so it could
// kick jDesc with new list of found chunks
func (jd *jDesc) applyChunkIds(ckIds []chunk.Id) error {
	jd.lock.Lock()

	if jd.state == jdStateClosed {
		jd.lock.Unlock()
		jd.logger.Error("applyChunkIds: for closed state ", jd)
		return util.ErrWrongState
	}

	// check whether something is needed
	if jd.isSameChunks(ckIds) {
		jd.lock.Unlock()
		jd.logger.Debug("applyChunkIds: all the chunks already known. Skipping the call.")
		return
	}

	jd.updateDescs(ckIds)
	if jd.state == jdStateStarted {
		jd.logger.Debug("applyChunkIds: Switching to starting")
		jd.setStarting()
	}
	jd.lock.Unlock()
	return nil
}

func (jd *jDesc) setStarting() {
	jd.state = jdStateStarting
	jd.crtCh = make(chan bool)
	go jd.syncChunksLists()
}

func (jd *jDesc) syncChunksLists() {
	jd.lock.Lock()
	if jd.state != jdStateStarting {
		jd.logger.Warn("syncChunksLists: unexpected state, skipping the check ", jd)
		jd.lock.Unlock()
		return
	}
	toCreate := jd.getChunksToOpen()
	jd.lock.Unlock()

	done := false
	created := false
	var jrnl journal.Journal
	for !done {
		// create new ones
		for _, cd := range toCreate {
			c, err := jd.jdc.newChunkById(jd, cd.cid)
			if err == nil {
				jd.onNewChunk(cd, c)
				created = true
			} else {
				jd.onNewChunkError(cd, err)
			}
		}

		jd.lock.Lock()
		if jd.state != jdStateStarting {
			jd.lock.Unlock()
			return
		}

		toCreate = jd.getChunksToOpen()
		if len(toCreate) == 0 {
			jrnl = jd.jrnl
			done = true
		}
		jd.lock.Unlock()
	}

	if jrnl == nil {
		jrnl = jd.jdc.newJournal(jd)
		created = true
	}

	jd.lock.Lock()
	if jd.state != jdStateStarting {
		jd.lock.Unlock()
		return
	}
	jd.rebuildChunksList()
	jd.state = jdStateStarted
	close(jd.crtCh)
	jd.lock.Unlock()
}

func (jd *jDesc) onNewChunk(cd *cDesc, c chunk.Chunk) {
	jd.lock.Lock()
	if cd.state != cStateChecking {
		jd.logger.Error("onNewChunk: Expecting cStateChecking, but ", cd)
		jd.lock.Unlock()
		c.Close()
		return
	}
	cd.state = cStateReady
	cd.chunk.chunk = c
	jd.AddListener(cd)
	jd.lock.Unlock()
}

func (jd *jDesc) onNewChunkError(cd *cDesc, err error) {
	jd.lock.Lock()
	if cd.state != cStateChecking {
		jd.logger.Error("onNewChunkError: Expecting cStateChecking, but ", cd)
		jd.lock.Unlock()
		return
	}
	cd.state = cStateError
	cd.chunk = nil
	jd.lock.Unlock()
}

func (jd *jDesc) AddListener(cd *cDesc) {
	if jd.jrnl != nil {
		lstnr, ok := jd.jrnl.(chunk.Listener)
		if ok {
			cd.chunk.AddListener(lstnr)
		}
	}
}

func (jd *jDesc) rebuildChunksList() {
	chunks := make([]chunk.Chunk, 0, nil)
	for _, cd := range jd.cDescs {
		if cd.state == cStateReady {
			chunks = append(chunks, cd.chunk)
		}
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Id() < chunks[j].Id() })
	jd.chunks = chunks
}

// isSameChunks gets list of chunks and checks whether they are already known
// returns true if the jd.cDescs contains exactly same ids
func (jd *jDesc) isSameChunks(ckIds []chunk.Id) bool {
	if len(jd.cDescs) != len(ckIds) {
		return false
	}

	for _, cid := range ckIds {
		if _, ok := jd.cDescs[cid]; !ok {
			return false
		}
	}

	return true
}

// updateDescs gets ckIds and updates jd.cDescs map with the new ids
func (jd *jDesc) updateDescs(ckIds []chunk.Id) {
	mp := make(map[chuk.Id]bool, len(ckIds))
	for _, cid := range ckIds {
		if _, ok := jd.cDescs[cid]; !ok {
			jd.cDescs[cid] = newCDesc(cid)
		}
		mp[cid] = true
	}

	for cid, cd := range jd.cDescs {
		if _, ok := mp[cid]; !ok {
			jd.deleteChunkInt(cd)
		}
	}
}

func (jd *jDesc) deleteChunkInt(cd *cDesc) {
	if cd.state == cStateHold || cd.state == cStateDeleted {
		jd.logger.Debug("chunk is deleted or being deleted cDesc=", cd)
		return
	}

	if cd.state != cStateReady {
		st := cd.state
		if cd.chunk != nil {
			cd.chunk.closeInternal()
			cd.chunk = nil
		}
		cd.state = cStateDeleted
		jd.logger.Info("The chunk descriptor is marked as deleted via ", cDescStateName(st), " state. cDesc=", cd)
		return
	}

	cd.state = cStateHold
	go func() {
		err := cd.chunk.rwLock.Lock()
		if err != nil {
			jd.logger.Warn("Could not complete deleting process, lock err=", err)
			return
		}
		cd.chunk.closeInternal()
		cd.chunk.rwLock.Unlock()
		jd.lock.Lock()
		jd.logger.Info("The chunk descriptor is marked as deleted through holding state cDesc=", cd)
		cd.chunk = nil
		cd.state = cStateDeleted
		jd.lock.Unlock()
	}()
}

func (jd *jDesc) getChunksToOpen() []*cDesc {
	res := make([]*cDesc, 0, len(jd.cDescs))
	for _, cd := range jd.cDescs {
		if cd.state == cStateNew {
			cd.state = cStateChecking
			cd.chunk = new(chunkWrapper)
			res = append(res, cd)
		}
	}
	return res
}
