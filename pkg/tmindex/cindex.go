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

package tmindex

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/range/pkg/records/chunk"
	sync2 "github.com/logrange/range/pkg/sync"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/logrange/range/pkg/utils/fileutil"
	"github.com/pkg/errors"
	"io/ioutil"
	"math"
	"path"
	"sync"
	"time"
)

type (
	// cindex struct allows to keep information about known chunks, this
	// naive implementation keeps everything in memory (but not ts index per chunk which is
	// stored in ckindex).
	cindex struct {
		lock sync.Mutex

		// journals build association between known partitions and its chunks
		journals   map[string]sortedChunks
		logger     log4g.Logger
		dtFileName string

		cc *ckiCtrlr
	}

	// sortedChunks slice contains information about a partition's chunks ordered by their chunkId
	sortedChunks []*chkInfo

	// chkInfo struct keeps information about a chunk. Used by cindex
	chkInfo struct {
		Id      chunk.Id
		MinTs   uint64
		MaxTs   uint64
		IdxRoot Item

		// the rwLock is used to access to the ckIndex
		rwLock  sync2.RWLock
		lastRec uint32
		// returns whether the ckIndex is considered
		idxCorrupted bool
	}
)

// cIndexFileName contains the name of file where cindex information will be persisted
const cIndexFileName = "cindex.dat"

// sparseSpace defines a coefficient how often to fix timestamp point in the index
const sparseSpace = 500

func newCIndex() *cindex {
	ci := new(cindex)
	ci.logger = log4g.GetLogger("cindex")
	ci.journals = make(map[string]sortedChunks)
	return ci
}

func (ci *cindex) init(ddir string) error {
	ci.logger.Info("Init()")
	err := fileutil.EnsureDirExists(ddir)
	if err != nil {
		ci.logger.Error("cindex.init(): could not create dir ", ddir, ", err=", err)
		return err
	}

	ci.dtFileName = path.Join(ddir, cIndexFileName)
	ci.logger.Info("Set data file to ", ci.dtFileName)
	ci.loadDataFromFile()

	ci.cc = newCkiCtrlr()
	err = ci.cc.init(ddir, 50)
	if err != nil {
		return err
	}

	aiMap := make(map[uint64]bool)
	for _, sc := range ci.journals {
		for _, c := range sc {
			if c.IdxRoot.IndexId != 0 {
				aiMap[c.IdxRoot.IndexId] = true
			}
		}
	}
	ci.cc.cleanup(aiMap)

	return nil
}

func (ci *cindex) close() error {
	err := ci.cc.close()
	ci.saveDataToFile()
	return err
}

// onWrite updates the partition cindex. It receives the partition name src, number of records in the
// chunk and the cinfo
func (ci *cindex) onWrite(src string, firstRec, lastRec uint32, rInfo RecordsInfo) (err error) {
	ci.lock.Lock()
	sc, ok := ci.journals[src]
	if !ok {
		sc = make(sortedChunks, 1)
		ci.journals[src] = sc
		sc[0] = &chkInfo{Id: rInfo.Id, MaxTs: rInfo.MaxTs, MinTs: rInfo.MinTs}
	}

	if sc[len(sc)-1].Id != rInfo.Id {
		// seems like we have a new chunk
		sc = append(sc, &chkInfo{Id: rInfo.Id, MaxTs: rInfo.MaxTs, MinTs: rInfo.MinTs})
		ci.journals[src] = sc
	} else if ok {
		sc[len(sc)-1].update(rInfo)
	}

	last := sc[len(sc)-1]
	ci.lock.Unlock()

	// now check whether we have to call for the index update
	if !last.rwLock.TryLock() {
		return nil
	}

	if last.idxCorrupted {
		last.rwLock.Unlock()
		return ErrTmIndexCorrupted
	}

	if last.lastRec > 0 && lastRec-last.lastRec < sparseSpace {
		// no need to write, give it a space so far
		last.rwLock.Unlock()
		return nil
	}

	if last.IdxRoot.IndexId == 0 {
		if lastRec-last.lastRec > sparseSpace*20 {
			// well, it is big gap in the index, let's make it rebuilt later...
			last.idxCorrupted = true
			last.IdxRoot = Item{}
			last.rwLock.Unlock()
			return ErrTmIndexCorrupted
		}

		root, err := ci.cc.arrangeRoot(record{int64(rInfo.MinTs), firstRec})
		if err != nil {
			ci.logger.Warn("could not create root element for ", rInfo, ", err=", err)
			last.IdxRoot = Item{}
			last.idxCorrupted = true
			last.rwLock.Unlock()
			return ErrTmIndexCorrupted
		}

		last.lastRec = lastRec
		last.IdxRoot = root
		last.rwLock.Unlock()
		return nil
	}

	last.IdxRoot, err = ci.cc.onWrite(last.IdxRoot, record{int64(rInfo.MinTs), firstRec})
	if err != nil {
		ci.logger.Warn("could not add new record to the index, err=", err, " last=", last)
		last.IdxRoot = Item{}
		last.idxCorrupted = true
	}
	last.lastRec = lastRec
	last.rwLock.Unlock()

	return err
}

func (ci *cindex) getPosForGreaterOrEqualTime(src string, cid chunk.Id, ts uint64) (uint32, error) {
	ci.lock.Lock()
	sc, ok := ci.journals[src]
	if !ok {
		ci.lock.Unlock()
		return 0, errors2.NotFound
	}

	idx := sc.findChunkIdx(cid)
	if idx < 0 {
		ci.lock.Unlock()
		return 0, errors2.NotFound
	}

	cinfo := sc[idx]
	if cinfo.MaxTs < ts {
		ci.lock.Unlock()
		return 0, ErrOutOfRange
	}

	if cinfo.MinTs >= ts {
		ci.lock.Unlock()
		return 0, nil
	}

	ci.lock.Unlock()

	cinfo.rwLock.RLock()
	if !cinfo.idxCorrupted {
		pos, err := ci.cc.grEq(cinfo.IdxRoot, int64(ts))
		if err == nil {
			cinfo.rwLock.RUnlock()
			return pos, nil
		}
	}
	cinfo.rwLock.RUnlock()

	return 0, ErrTmIndexCorrupted
}

func (ci *cindex) getPosForLessTime(src string, cid chunk.Id, ts uint64) (uint32, error) {
	ci.lock.Lock()
	sc, ok := ci.journals[src]
	if !ok {
		ci.lock.Unlock()
		return 0, errors2.NotFound
	}

	idx := sc.findChunkIdx(cid)
	if idx < 0 {
		ci.lock.Unlock()
		return 0, errors2.NotFound
	}

	cinfo := sc[idx]
	if cinfo.MaxTs <= ts {
		ci.lock.Unlock()
		return math.MaxUint32, nil
	}

	if cinfo.MinTs >= ts {
		ci.lock.Unlock()
		return 0, ErrOutOfRange
	}

	ci.lock.Unlock()

	cinfo.rwLock.RLock()
	if !cinfo.idxCorrupted {
		pos, err := ci.cc.less(cinfo.IdxRoot, int64(ts))
		cinfo.rwLock.RUnlock()
		if err == nil {
			return pos, nil
		}
		// whell, in case the error, we are still not sure where to start, so just start from the end
		return math.MaxUint32, nil
	}
	cinfo.rwLock.RUnlock()

	return 0, ErrTmIndexCorrupted

}

// lastChunkRecordsInfo returns the last chunk information. Returns NotFound if there is
// no such source
func (ci *cindex) lastChunkRecordsInfo(src string) (res RecordsInfo, err error) {
	err = errors2.NotFound
	ci.lock.Lock()
	if sc, ok := ci.journals[src]; ok && len(sc) > 0 {
		res = sc[len(sc)-1].getRecordsInfo()
		err = nil
	}
	ci.lock.Unlock()
	return
}

// rebuildIndex runs the building new index for the source and the chunk provided
func (ci *cindex) rebuildIndex(ctx context.Context, src string, chk chunk.Chunk) {
	ci.lock.Lock()
	var res *chkInfo
	if sc, ok := ci.journals[src]; ok {
		for _, c := range sc {
			if c.Id == chk.Id() {
				res = c
				break
			}
		}
	}
	ci.lock.Unlock()

	if res == nil {
		ci.logger.Error("rebuildIndex(): no partiton ", src, ", or such chunk there")
		return
	}

	if err := res.rwLock.LockWithCtx(ctx); err != nil {
		ci.logger.Error("rebuildIndex(): could not acquire write lock, err=", err)
		return
	}

	if res.IdxRoot.IndexId != 0 && !res.idxCorrupted {
		if ci.cc.getIndex(res.IdxRoot.IndexId) != nil {
			ci.logger.Warn("The index for chunk ", chk, " for partition \"", src, "\" seems to be ok and alive. Do not build one. ")
			res.rwLock.Unlock()
			return
		}
		res.IdxRoot = Item{}
		res.idxCorrupted = true
	}

	rInfo, root, err := ci.rebuildIndexInt(ctx, chk)
	if err != nil || root.IndexId == 0 {
		res.rwLock.Unlock()
		return
	}

	res.IdxRoot = root
	res.idxCorrupted = false
	res.lastRec = 0
	res.rwLock.Unlock()

	// To change the rInfo acquire another lock
	ci.lock.Lock()
	found := false
	if sc, ok := ci.journals[src]; ok {
		for _, c := range sc {
			if c == res {
				found = true
				res.update(rInfo)
				break
			}
		}
	}
	ci.lock.Unlock()

	if !found {
		ci.logger.Warn("rebuildIndex(): chunk either disappear or partition was deleted ", chk, " partition ", src)
		ci.cc.removeItem(root)
	}
}

func (ci *cindex) rebuildIndexInt(ctx context.Context, chk chunk.Chunk) (RecordsInfo, Item, error) {
	var rInfo RecordsInfo
	var root Item

	rInfo.Id = chk.Id()

	if chk.Count() > 0 {
		it, err := chk.Iterator()
		if err != nil {
			ci.logger.Error("rebuildIndexInt(): could not create iterator, err=", err)
			return rInfo, root, err
		}
		defer it.Close()

		ts, err := getRecordTimestamp(ctx, it)
		if err != nil {
			ci.logger.Error("rebuildIndexInt(): could not read first record, err=", err)
			return rInfo, root, err
		}

		rInfo.MinTs = ts
		rInfo.MaxTs = ts
		root, err = ci.cc.arrangeRoot(record{int64(ts), 0})
		if err != nil {
			ci.logger.Warn("rebuildIndexInt(): could not create root element for ", rInfo, ", err=", err)
			return rInfo, root, err
		}

		pos := int64(sparseSpace)
		doRun := true
		for doRun {
			if pos >= int64(chk.Count()) {
				// set pos to the last record
				pos = int64(chk.Count()) - 1
				doRun = false
			}

			it.SetPos(pos)
			ts, err := getRecordTimestamp(ctx, it)
			if err != nil {
				ci.cc.removeItem(root)
				ci.logger.Error("rebuildIndexInt(): could not read timestamp for position", pos, ", err=", err)
				return rInfo, root, err
			}

			root, err = ci.cc.onWrite(root, record{int64(ts), uint32(pos)})
			if err != nil {
				ci.cc.removeItem(root)
				ci.logger.Error("rebuildIndexInt(): could not write index info for ", pos, ", err=", err)
				return rInfo, root, err
			}
			if rInfo.MinTs > ts {
				rInfo.MinTs = ts
			}
			if rInfo.MaxTs < ts {
				rInfo.MaxTs = ts
			}

			pos += sparseSpace
		}
	} else {
		ci.logger.Info("rebuildIndex(): the chunk ", chk, " has 0 size")
	}

	return rInfo, root, nil
}

// syncChunks receives a list of chunks for a partition src and updates the cindex information by the data from the chunks
func (ci *cindex) syncChunks(ctx context.Context, src string, cks chunk.Chunks) []RecordsInfo {
	newSC := chunksToSortedChunks(cks)

	ci.lock.Lock()
	if sc, ok := ci.journals[src]; ok {
		// re-assignment cause apply can re-allocate original slice
		newSC, _ = newSC.apply(sc, true)
	}
	ci.lock.Unlock()

	ci.lightFill(ctx, cks, newSC)

	ci.lock.Lock()
	sc := ci.journals[src]
	newSC, rmvd := newSC.apply(sc, false)
	ci.journals[src] = newSC
	res := newSC.makeRecordsInfoCopy()
	ci.lock.Unlock()

	ci.dropSortedChunks(ctx, rmvd)
	return res
}

func (ci *cindex) dropSortedChunks(ctx context.Context, rmvd sortedChunks) {
	for _, r := range rmvd {
		if err := r.rwLock.LockWithCtx(ctx); err != nil {
			ci.logger.Error("Could not obtain read lock. potential leak!!!")
			go ci.cc.removeIndex(r.IdxRoot.IndexId)
			continue
		}

		if r.IdxRoot.IndexId != 0 {
			ci.logger.Debug("Remove tm index for chunk ", r.Id)
			ci.cc.removeItem(r.IdxRoot)
		}
		r.rwLock.Unlock()
	}
}

func (ci *cindex) lightFill(ctx context.Context, cks chunk.Chunks, sc sortedChunks) {
	j := 0
	for _, c := range sc {
		if c.MaxTs > 0 {
			continue
		}

		for ; j < len(cks) && cks[j].Id() < c.Id; j++ {
		}

		if j == len(cks) {
			return
		}

		chk := cks[j]
		if chk.Id() != c.Id {
			panic(fmt.Sprintf("lightFill(): could not find chunk for id=%v check the code!", c.Id))
		}

		if chk.Count() == 0 {
			ci.logger.Debug("lightFill(): empty chunk, continue.")
			continue
		}

		it, err := chk.Iterator()
		if err != nil {
			ci.logger.Error("lightFill(): Could not create iterator, err=", err)
			continue
		}

		ts1, err := getRecordTimestamp(ctx, it)
		if err != nil {
			it.Close()
			ci.logger.Warn("lightFill(): Could not read first record, err=", err)
			continue
		}

		it.SetPos(int64(chk.Count()) - 1)
		ts2, err := getRecordTimestamp(ctx, it)
		if err != nil {
			it.Close()
			ci.logger.Warn("lightFill(): Could not read last record, err=", err)
			continue
		}

		c.MinTs = ts1
		c.MaxTs = ts2
		if ts2 < ts1 {
			ci.logger.Warn("lightFill(): first record of chunk ", chk.Id(), " has greater timestamp, thant its last one")
			c.MinTs = ts2
			c.MaxTs = ts1
		}
		it.Close()
	}
}

func getRecordTimestamp(ctx context.Context, it chunk.Iterator) (uint64, error) {
	rec, err := it.Get(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "getRecordTimestamp(): could not read record")
	}

	var le model.LogEvent
	_, err = le.Unmarshal(rec, false)
	if err != nil {
		return 0, errors.Wrapf(err, "getRecordTimestamp(): could not unmarshal record")
	}
	return le.Timestamp, nil
}

func (ci *cindex) loadDataFromFile() {
	data, err := ioutil.ReadFile(ci.dtFileName)
	if err != nil {
		ci.logger.Warn("loadDataFromFile(): could not read data from file, the err=", err)
		return
	}

	err = json.Unmarshal(data, &ci.journals)
	if err != nil {
		ci.logger.Warn("loadDataFromFile(): could not unmarshal data. err=", err)
		return
	}
	ci.logger.Info("successfully read information about ", len(ci.journals), " journals from ", ci.dtFileName)
}

func (ci *cindex) saveDataToFile() {
	if len(ci.dtFileName) == 0 {
		ci.logger.Warn("Could not persist data, no file name.")
		return
	}

	data, err := json.Marshal(ci.journals)
	if err != nil {
		ci.logger.Error("Could not persist data to file ", ci.dtFileName, ", err=", err)
		return
	}

	if err = ioutil.WriteFile(ci.dtFileName, data, 0640); err != nil {
		ci.logger.Error("could not save data to ", ci.dtFileName, ", err=", err)
		return
	}

	ci.logger.Info("saved all data to ", ci.dtFileName)
}

func chunksToSortedChunks(cks chunk.Chunks) sortedChunks {
	res := make(sortedChunks, len(cks))
	for i, ck := range cks {
		res[i] = &chkInfo{Id: ck.Id(), MinTs: math.MaxUint64}
	}
	return res
}

// apply overwrites values in sc by values sc1. It returns 2 sorted slices
// the firs one is the new merged chunks and the second one is sorted chunks
// that were removed from sc.
func (sc sortedChunks) apply(sc1 sortedChunks, copyData bool) (sortedChunks, sortedChunks) {
	i := 0
	j := 0
	removed := make(sortedChunks, 0, 1)
	for i < len(sc) && j < len(sc1) {
		if sc[i].Id > sc1[j].Id {
			j++
			removed = append(removed, sc1[j])
			continue
		}
		if sc[i].Id < sc1[j].Id {
			i++
			continue
		}
		if copyData {
			sc[i].MaxTs = sc1[j].MaxTs
			sc[i].MinTs = sc1[j].MinTs
		} else {
			sc[i] = sc1[j]
		}
		i++
		j++
	}

	// add all tailed chunks that are not in sc to sc
	for ; j < len(sc1); j++ {
		ci := sc1[j]
		if copyData {
			ci = new(chkInfo)
			*ci = *sc1[j]
		}
		sc = append(sc, ci)
	}
	// returns sc cause it could re-allocate original slice
	return sc, removed
}

func (sc sortedChunks) makeRecordsInfoCopy() []RecordsInfo {
	res := make([]RecordsInfo, len(sc))
	for i, c := range sc {
		res[i] = c.getRecordsInfo()
	}
	return res
}

func (sc sortedChunks) findChunkIdx(cid chunk.Id) int {
	i, j := 0, len(sc)
	for i < j {
		h := int(uint(i+j) >> 1)
		if sc[h].Id == cid {
			return h
		}

		if sc[h].Id < cid {
			i = h + 1
		} else {
			j = h
		}
	}
	return -1
}

func (ci *chkInfo) getRecordsInfo() RecordsInfo {
	return RecordsInfo{Id: ci.Id, MinTs: ci.MinTs, MaxTs: ci.MaxTs}
}

func (ci *chkInfo) update(rInfo RecordsInfo) bool {
	if ci.MinTs > rInfo.MinTs {
		ci.MinTs = rInfo.MinTs
	}
	if ci.MaxTs < rInfo.MaxTs {
		ci.MaxTs = rInfo.MaxTs
	}
	return true
}

func (ci *chkInfo) String() string {
	minTs := time.Unix(0, int64(ci.MinTs))
	maxTs := time.Unix(0, int64(ci.MaxTs))
	return fmt.Sprintf("chkInfo:{Id: %s, MinTs: %s, MaxTs: %s, IdxRoot: %s, lastRec: %d, corrupted: %t}", ci.Id, minTs, maxTs, ci.IdxRoot, ci.lastRec, ci.idxCorrupted)
}
