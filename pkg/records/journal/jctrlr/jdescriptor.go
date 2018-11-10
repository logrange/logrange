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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/records/journal"
)

type (
	jDescCtrlr interface {
		newChunkById(jd *jDescriptor, cid chunk.Id) (chunk.Chunk, error)
		newJournal(jd *jDescriptor) journal.Journal
		onChunksCreated(jd *jDescriptor)
	}

	jDescriptor struct {
		lock sync.Mutex
		// name is the journal name
		name string
		// dir is the directory of the journal
		dir string
		// known the journal chunks
		chnkIds []chunk.Id
		// current state
		state int

		jdc    jDescCtrlr
		chunks atomic.Value
		jrnl   journal.Journal

		crtCh chan bool
	}
)

const (
	cJDS_INIT     = 0
	cJDS_CREATING = 1
	cJDS_CREATED  = 2
)

var (
	emptyChunks = make(chunk.Chunks, 0)
)

func newJDescriptor(jdc jDescCtrlr, sj scJournal) *jDescriptor {
	jd := new(jDescriptor)
	jd.name = sj.name
	jd.dir = sj.dir
	jd.chunks.Store(emptyChunks)
	jd.chnkIds = sj.chunks
	jd.state = cJDS_INIT
	jd.jdc = jdc
	return jd
}

func (jd *jDescriptor) String() string {
	return fmt.Sprintf("{name=%s, state=%d, chunks=%d, dir=%s}", jd.name, jd.state, len(jd.chunks.Load().(chunk.Chunks)), jd.dir)
}

func (jd *jDescriptor) applyChunkIds(ckIds []chunk.Id) {
	jd.lock.Lock()
	jd.chnkIds = ckIds
	if jd.state == cJDS_CREATED {
		jd.setCreating()
	}
	jd.lock.Unlock()
}

func (jd *jDescriptor) setCreating() {
	jd.state = cJDS_CREATING
	jd.crtCh = make(chan bool)
	go jd.syncChunksLists()
}

func (jd *jDescriptor) getOrCreateJournal(ctx context.Context) (journal.Journal, error) {
	var ch chan bool

	jd.lock.Lock()
	switch jd.state {
	case cJDS_INIT:
		jd.setCreating()
	case cJDS_CREATING:
		ch = jd.crtCh
	}

	jrnl := jd.jrnl
	jd.lock.Unlock()

	if jrnl != nil || ch == nil {
		return jrnl, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ch:
	}

	jd.lock.Lock()
	jrnl = jd.jrnl
	jd.lock.Unlock()
	return jrnl, nil
}

func (jd *jDescriptor) Close() error {
	var chunks chunk.Chunks
	var jrnl journal.Journal
	jd.lock.Lock()
	if jd.state != cJDS_INIT {
		if jd.state == cJDS_CREATING {
			close(jd.crtCh)
		}
		jd.state = cJDS_INIT
		chunks = jd.chunks.Load().(chunk.Chunks)
		jrnl = jd.jrnl
		jd.chunks.Store(emptyChunks)
		jd.jrnl = nil
	}
	jd.lock.Unlock()

	if jrnl != nil {
		jrnl.Close()
	}

	for _, c := range chunks {
		c.Close()
	}

	return nil
}

func (jd *jDescriptor) syncChunksLists() {
	jd.lock.Lock()
	if jd.state != cJDS_CREATING {
		jd.lock.Unlock()
		return
	}
	toClose := jd.getChunksToClose()
	toCreate := jd.getChunksIdsToCreate()
	jd.lock.Unlock()

	done := false
	created := false
	var jrnl journal.Journal
	for !done {
		// close all from toClose
		for _, c := range toClose {
			c.Close()
		}

		// create new ones
		newChks := make(chunk.Chunks, 0, len(toCreate))
		for _, cid := range toCreate {
			c, err := jd.jdc.newChunkById(jd, cid)
			if err == nil {
				newChks = append(newChks, c)
				created = true
			}
		}

		jd.lock.Lock()
		if jd.state != cJDS_CREATING {
			jd.lock.Unlock()
			return
		}
		jd.appendNewChunks(newChks)
		toClose = jd.getChunksToClose()
		toCreate = jd.getChunksIdsToCreate()
		if len(toClose) == 0 && len(toCreate) == 0 {
			jrnl = jd.jrnl
			done = true
		}
		jd.lock.Unlock()
	}

	if jrnl == nil {
		jrnl = jd.jdc.newJournal(jd)
	} else if created {
		jd.jdc.onChunksCreated(jd)
	}

	jd.lock.Lock()
	if jd.state != cJDS_CREATING {
		jd.lock.Unlock()
		return
	}
	jd.onNewJournal(jrnl)
	jd.state = cJDS_CREATED
	close(jd.crtCh)

	jd.lock.Unlock()
}

func (jd *jDescriptor) onNewJournal(jrnl journal.Journal) {
	if jd.jrnl == jrnl {
		return
	}

	chunks := jd.chunks.Load().(chunk.Chunks)
	jd.jrnl = jrnl
	lstnr := jrnl.(chunk.Listener)
	for _, c := range chunks {
		c.AddListener(lstnr)
	}
}

func (jd *jDescriptor) appendNewChunk(ck chunk.Chunk) {
	jd.appendNewChunks(chunk.Chunks{ck})
}

func (jd *jDescriptor) appendNewChunks(cks2Add chunk.Chunks) {
	chunks := jd.chunks.Load().(chunk.Chunks)
	newChks := make(chunk.Chunks, 0, len(chunks)+len(cks2Add))
	newChks = append(newChks, chunks...)
	newChks = append(newChks, cks2Add...)
	sort.Slice(newChks, func(i, j int) bool { return newChks[i].Id() < newChks[j].Id() })
	jd.chunks.Store(newChks)
}

func (jd *jDescriptor) getChunksIdMap() map[chunk.Id]bool {
	cksMap := make(map[chunk.Id]bool, len(jd.chnkIds))
	for _, id := range jd.chnkIds {
		cksMap[id] = true
	}
	return cksMap
}

// getChunksIdsToCreate returns slice of chunk Ids that must be created
func (jd *jDescriptor) getChunksIdsToCreate() []chunk.Id {
	cksIdMap := jd.getChunksIdMap()
	chunks := jd.chunks.Load().(chunk.Chunks)

	for _, c := range chunks {
		delete(cksIdMap, c.Id())
	}

	res := make([]chunk.Id, 0, len(cksIdMap))
	for id := range cksIdMap {
		res = append(res, id)
	}

	return res
}

// getChunksToClose compares jd.chunks slice with jd.chnkIds and removes
// all chunks from the first slice which Ids are not in the second one. It
// returns slice of the removed chunks
func (jd *jDescriptor) getChunksToClose() chunk.Chunks {
	cksIdMap := jd.getChunksIdMap()
	chunks := jd.chunks.Load().(chunk.Chunks)
	newChnks := make(chunk.Chunks, 0, len(chunks))
	res := make(chunk.Chunks, 0, len(chunks))
	for _, c := range chunks {
		if _, ok := cksIdMap[c.Id()]; !ok {
			res = append(res, c)
		} else {
			newChnks = append(newChnks, c)
		}
	}

	if len(res) > 0 {
		jd.chunks.Store(newChnks)
	}
	return res
}
