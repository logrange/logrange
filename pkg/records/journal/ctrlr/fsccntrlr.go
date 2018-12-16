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

package ctrlr

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jrivets/log4g"
	lctx "github.com/logrange/logrange/pkg/context"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/records/chunk/chunkfs"
	"github.com/logrange/logrange/pkg/util"
	"github.com/pkg/errors"
)

type (
	fsChnksController struct {
		dir    string
		fdPool *chunkfs.FdPool
		lock   sync.Mutex
		state  int32
		logger log4g.Logger

		// config for new chunks
		chunkCfg chunkfs.Config

		// known chunks
		knwnChunks map[chunk.Id]*fsChnkInfo

		// chunks is a sorted slice of []chunk.Chunk
		chunks []chunk.Chunk

		startingCh chan struct{}

		// dirSem semaphore controls access to the dir changes and scans
		dirSem chan bool

		ckListener chunk.Listener
	}

	fsChnkInfo struct {
		state int
		chunk *chunkWrapper
	}
)

const (
	fsCCStateNew = iota
	fsCCStateScanned
	fsCCStateStarting
	fsCCStateStarted
	fsCCStateClosed
)

// A chunk desc states
const (
	// Scanned - found a file on the disk, but it is not checked
	fsChunkStateScanned = iota
	// Error - Checked and could not be recovered
	fsChunkStateError
	// Ok - checked and chunk was created, normal one
	fsChunkStateOk
	// Deleted - chunk was going to be removed from the dir (controlled by external)
	// procedure
	fsChunkStateDeleted
	// Deleting means that chunk is under deleting at the moment
	fsChunkStateDeleting
	// Phantom - sets by scanner. Indicates that there is a record, but no data
	// file anymore. The chunk will be closed and collected later...
	fsChunkStatePhantom
)

var ccStateNames = map[int]string{
	fsCCStateNew:      "NEW",
	fsCCStateScanned:  "SCANNED",
	fsCCStateStarting: "STARTING",
	fsCCStateStarted:  "STARTED",
	fsCCStateClosed:   "CLOSED",
}

var ccChunkStateNames = map[int]string{
	fsChunkStateScanned:  "SCANNED",
	fsChunkStateError:    "ERROR",
	fsChunkStateOk:       "OK",
	fsChunkStateDeleted:  "DELETED",
	fsChunkStateDeleting: "DELETING",
	fsChunkStatePhantom:  "PHANTOM",
}

func ccStateName(state int32) string {
	if name, ok := ccStateNames[int(state)]; ok {
		return name
	}
	return fmt.Sprintf("Unknown cc state=%d", state)
}

func ccChunkStateName(state int) string {
	if name, ok := ccChunkStateNames[state]; ok {
		return name
	}
	return fmt.Sprintf("Unknown chunk state=%d", state)
}

func newFSChnksController(name, dir string, fdPool *chunkfs.FdPool, chunkCfg chunkfs.Config) *fsChnksController {
	fc := new(fsChnksController)
	fc.dir = dir
	fc.fdPool = fdPool
	fc.chunkCfg = chunkCfg
	fc.state = fsCCStateNew
	fc.logger = log4g.GetLogger("fsChnksController").WithId("{" + name + "}").(log4g.Logger)
	fc.dirSem = make(chan bool, 1)
	fc.dirSem <- true
	return fc
}

func (fc *fsChnksController) ensureInit() {
	if atomic.LoadInt32(&fc.state) == fsCCStateNew {
		fc.scan()
	}
}

// scan checks the dir to detect chunk files there and build a list of chunk Ids.
// It returns the list of chunk Ids that could be advertised (see getAdvChunks)
func (fc *fsChnksController) scan() ([]chunk.Id, error) {
	// acquire semaphore for the dir
	if err := fc.acquireSem(); err != nil {
		return nil, err
	}
	defer fc.releaseSem()

	cks, err := scanForChunks(fc.dir)
	if err != nil {
		return nil, err
	}

	fc.lock.Lock()
	if fc.state == fsCCStateNew {
		atomic.StoreInt32(&fc.state, fsCCStateScanned)
		fc.knwnChunks = make(map[chunk.Id]*fsChnkInfo)
		for _, cid := range cks {
			fc.knwnChunks[cid] = &fsChnkInfo{state: fsChunkStateScanned}
		}
		fc.lock.Unlock()
		return fc.getAdvChunks(), nil
	}

	if fc.state == fsCCStateClosed {
		fc.lock.Unlock()
		return nil, util.ErrWrongState
	}

	update := false
	cidMap := make(map[chunk.Id]bool, len(cks))
	for _, cid := range cks {
		cidMap[cid] = true
		_, ok := fc.knwnChunks[cid]
		if !ok {
			fc.knwnChunks[cid] = &fsChnkInfo{state: fsChunkStateScanned}
			update = true
		}
	}

	for cid, ci := range fc.knwnChunks {
		if _, ok := cidMap[cid]; !ok && ci.state != fsChunkStateDeleting && ci.state != fsChunkStateDeleted {
			fc.logger.Warn("scan(): found chunk phantom ", cid, " no data in the dir, but there is a record in knwnChunks")
			if ci.chunk != nil {
				// to update chunks slice if it is there...
				update = update || ci.state == fsChunkStateOk

				go fc.closePhantom(ci.chunk)
				ci.state = fsChunkStateDeleting
			} else {
				delete(fc.knwnChunks, cid)
			}
		}
	}

	if update && fc.state == fsCCStateStarted {
		fc.switchStarting()
	}
	fc.lock.Unlock()

	return fc.getAdvChunks(), nil
}

func (fc *fsChnksController) closePhantom(chk *chunkWrapper) {
	fc.logger.Info("Deleting phantom chunk ", chk)
	err := chk.rwLock.LockWithCtx(nil)
	if err != nil {
		fc.logger.Warn("closePhantom(): Could not acquire Write lock for chunkWrapper ", chk, ", err=", err, ". Interrupting.")
		return
	}

	cid := chk.Id()
	err = chk.closeInternal()
	if err != nil {
		fc.logger.Warn("closePhantom(): closeInternal returns err=", err)
	}
	fc.lock.Lock()
	defer fc.lock.Unlock()

	fc.logger.Info("Deleting phantom chunk ", cid)
	delete(fc.knwnChunks, cid)
}

// getAdvChunks returns sorted slice of chunks that can be advertised like the journal ones
func (fc *fsChnksController) getAdvChunks() []chunk.Id {
	fc.lock.Lock()
	res := make([]chunk.Id, 0, len(fc.knwnChunks))
	for cid, ci := range fc.knwnChunks {
		if ci.state == fsChunkStateScanned || ci.state == fsChunkStateOk {
			res = append(res, cid)
		}
	}
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	fc.lock.Unlock()
	return res
}

func (fc *fsChnksController) close() error {
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if fc.state == fsCCStateClosed {
		return util.ErrWrongState
	}

	close(fc.dirSem)

	if fc.state == fsCCStateStarting {
		close(fc.startingCh)
	}

	kc := fc.knwnChunks
	fc.knwnChunks = nil
	go func() {
		for _, ci := range kc {
			if ci.chunk != nil {
				ci.chunk.closeInternal()
			}
		}
	}()

	atomic.StoreInt32(&fc.state, fsCCStateClosed)
	return nil
}

// getChunkForWrite returns last chunk if it is ok for write, or create a new one and
// returns the new one. If the new one was created it returns true in the second param
func (fc *fsChnksController) getChunkForWrite(ctx context.Context) (chunk.Chunk, bool, error) {
	ck, err := fc.getLastChunk(ctx)
	if err != nil {
		return nil, false, err
	}

	if ck != nil && ck.Size() < fc.chunkCfg.MaxChunkSize {
		return ck, false, nil
	}

	ck, err = fc.createChunkForWrite(ctx)
	if ck != nil || err != nil {
		return ck, false, err
	}

	// must wait till constructed
	ck, err = fc.getLastChunk(ctx)
	return ck, true, err
}

func (fc *fsChnksController) createChunkForWrite(ctx context.Context) (chunk.Chunk, error) {
	// acquire sem
	if err := fc.acquireSem(); err != nil {
		return nil, err
	}
	defer fc.releaseSem()

	ck, err := fc.getLastChunk(ctx)
	if err != nil {
		return nil, err
	}

	// another call could create the new chunk
	if ck != nil && ck.Size() < fc.chunkCfg.MaxChunkSize {
		return ck, nil
	}

	cfg := fc.chunkCfg
	cfg.Id = chunk.NewId()
	cfg.FileName = chunkfs.MakeChunkFileName(fc.dir, cfg.Id)
	// new chunk, so disabling check
	cfg.CheckDisabled = true
	err = chunkfs.EnsureFilesExist(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not create files for the new chunk %s", cfg.FileName)
	}

	fc.lock.Lock()
	fc.knwnChunks[cfg.Id] = &fsChnkInfo{state: fsChunkStateScanned}
	if fc.state == fsCCStateStarted {
		fc.switchStarting()
	}
	fc.lock.Unlock()
	// we return nil for chunk intentionally here, to indicate that a new one was created
	return nil, nil
}

// getChunks returns sorted sllice of known journal chunks
func (fc *fsChnksController) getChunks(ctx context.Context) (chunk.Chunks, error) {
	for ctx.Err() == nil {
		fc.lock.Lock()
		if fc.state == fsCCStateStarted {
			res := fc.chunks
			fc.lock.Unlock()
			return res, nil
		}

		if fc.state == fsCCStateClosed || fc.state == fsCCStateNew {
			fc.lock.Unlock()
			return nil, util.ErrWrongState
		}

		if fc.state == fsCCStateScanned {
			fc.switchStarting()
		}

		ch := fc.startingCh
		fc.lock.Unlock()

		// it waits till it will be swtching to Started
		select {
		case <-ctx.Done():
		case <-ch:
		}
	}
	return nil, ctx.Err()
}

func (fc *fsChnksController) getLastChunk(ctx context.Context) (chunk.Chunk, error) {
	cks, err := fc.getChunks(ctx)
	if err != nil {
		return nil, err
	}

	if len(cks) > 0 {
		return cks[len(cks)-1], nil
	}
	return nil, nil
}

func (fc *fsChnksController) acquireSem() error {
	_, ok := <-fc.dirSem
	if !ok {
		return util.ErrWrongState
	}
	return nil
}

func (fc *fsChnksController) releaseSem() {
	fc.lock.Lock()
	if fc.state != fsCCStateClosed {
		fc.dirSem <- true
	}
	fc.lock.Unlock()
}

func (fc *fsChnksController) switchStarting() {
	fc.logger.Debug("Switch starting")
	atomic.StoreInt32(&fc.state, fsCCStateStarting)
	fc.startingCh = make(chan struct{})
	go fc.syncChunks()
}

func (fc *fsChnksController) syncChunks() {
	fc.logger.Debug("syncChunks() starting")
	var toCreate map[chunk.Id]fsChnkInfo
	for {
		fc.lock.Lock()
		if fc.state != fsCCStateStarting {
			fc.logger.Warn("syncChunks(): unexpected state ", ccStateName(fc.state), ", but expecting STARTING. interrupting.")
			fc.lock.Unlock()
			return
		}

		// apply setting from previous round
		for cid, ci := range toCreate {
			fci, ok := fc.knwnChunks[cid]
			if !ok {
				fc.logger.Warn("syncChunks(): Chunk with ", cid, " was created, but it was removed from knwnChunks map. closing it if needed")
				if ci.state == fsChunkStateScanned {
					ci.chunk.closeInternal()
				}
				continue
			}

			if fci.state != fsChunkStateScanned {
				fc.logger.Warn("syncChunks(): Expecting chunk ", cid, " state to be SCANNED, but it is ", ccChunkStateName(fci.state))
				continue
			}

			*fci = ci
		}

		// define chunks to be constructed for the round
		oks := 0
		toCreate = make(map[chunk.Id]fsChnkInfo, len(fc.knwnChunks))
		for cid, fci := range fc.knwnChunks {
			if fci.state == fsChunkStateOk {
				oks++
			}

			if fci.state != fsChunkStateScanned {
				continue
			}

			toCreate[cid] = *fci
		}

		// if nothing to create
		if len(toCreate) == 0 {
			fc.chunks = make(chunk.Chunks, 0, oks)
			for _, fci := range fc.knwnChunks {
				if fci.state == fsChunkStateOk {
					fc.chunks = append(fc.chunks, fci.chunk)
				}
			}
			sort.Slice(fc.chunks, func(i, j int) bool { return fc.chunks[i].Id() < fc.chunks[j].Id() })
			atomic.StoreInt32(&fc.state, fsCCStateStarted)
			close(fc.startingCh)
			fc.lock.Unlock()
			return
		}

		// got cancelled contex by the fc.startingCh channel
		ctx := lctx.WrapChannel(fc.startingCh)
		fc.lock.Unlock()

		fc.logger.Debug("syncChunks(): Going to create ", len(toCreate), " chunks")
		for cid, ci := range toCreate {
			if ctx.Err() != nil {
				fc.logger.Warn("syncChunks(): context cancelled, while creating ", len(toCreate), " chunks")
				return
			}
			cfg := fc.chunkCfg
			cfg.Id = cid
			cfg.FileName = chunkfs.MakeChunkFileName(fc.dir, cfg.Id)
			c, err := chunkfs.New(ctx, cfg, fc.fdPool)
			if err != nil {
				fc.logger.Warn("syncChunks(): could not create new chunk ", cid, " err=", err)
				ci.state = fsChunkStateError
			} else {
				fc.logger.Debug("syncChunks(): New chunk ", cid, " successfully created")
				ci.state = fsChunkStateOk
				ci.chunk = new(chunkWrapper)
				ci.chunk.chunk = c
				ci.chunk.AddListener(fc.ckListener)
			}
			toCreate[cid] = ci
		}
	}
}
