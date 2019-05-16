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
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/range/pkg/bstorage"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/logrange/range/pkg/utils/fileutil"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type (
	ckiCtrlr struct {
		logger log4g.Logger

		idxes      atomic.Value
		createLock sync.Mutex
		ddir       string
		maxSegms   int
	}

	ckiMap map[uint64]*ckindex

	// Item struct identifies an element in the timestamp tree. It points to root
	// block where data is stored.
	Item struct {
		// IndexId contains the index file identifier. 0 means not assigned yet
		IndexId uint64
		// Pos identifies the root block number
		Pos int
	}
)

const (
	idxFileExt = ".tidx"
)

var (
	emptyMap = make(ckiMap, 0)
)

func newCkiCtrlr() *ckiCtrlr {
	cc := new(ckiCtrlr)
	cc.logger = log4g.GetLogger("ckiCtrlr")
	cc.idxes.Store(emptyMap) // put empty map so far
	return cc
}

func (cc *ckiCtrlr) init(ddir string, maxSegms int) error {
	err := fileutil.EnsureDirExists(ddir)
	if err != nil {
		cc.logger.Error("could not open or create ", ddir, ", err=", err)
		return err
	}

	ids, err := getListOfExistingIndexFiles(ddir)
	if err != nil {
		cc.logger.Error("could not scan for index files, err=", err)
		return err
	}
	cc.ddir = ddir
	cc.maxSegms = maxSegms

	cc.logger.Info("Found ", len(ids), " files for indexes: ", ids)

	// creating the found indexes now
	err = cc.createIndexes(ids)
	if err != nil {
		cc.close()
	}

	return err
}

// cleanup allows to remove index files, that are not used
func (cc *ckiCtrlr) cleanup(knownIndexes map[uint64]bool) {
	mp := cc.idxes.Load().(ckiMap)
	toRemove := make([]uint64, 0, len(mp))
	for ii := range mp {
		if _, ok := knownIndexes[ii]; !ok {
			toRemove = append(toRemove, ii)
		}
	}

	for ii := range knownIndexes {
		if _, ok := mp[ii]; !ok {
			cc.logger.Warn("Expected to have index ", ii, ", but it is not found")
		}
	}

	if len(toRemove) == 0 {
		cc.logger.Info("cleanup(): expected to have ", knownIndexes, " 0 to clean up")
		return
	}

	cc.logger.Info("cleanup(): expected to have ", knownIndexes, " and the following must be removed: ", toRemove)

	for _, ii := range toRemove {
		cc.removeIndex(ii)
	}

}

// arrangeRoot allocates a root block in one of the known indexes, returns
// its position with one recrord rec, or an error if any
func (cc *ckiCtrlr) arrangeRoot(it interval) (Item, error) {
	id, cki, err := cc.allocateIndex()
	if err != nil {
		return Item{}, err
	}

	pos, err := cki.addInterval(-1, it)
	if err != nil {
		return Item{}, err
	}

	return Item{id, pos}, nil
}

// onWrite notifies about writing data to a chunk. root describes the root
// block for the index tree.
func (cc *ckiCtrlr) onWrite(root Item, it interval) (Item, error) {
	cki := cc.getIndex(root.IndexId)
	if cki == nil {
		return root, errors2.NotFound
	}

	err := cc.extend(cki)
	if err != nil {
		cc.logger.Warn("error while extending index storage ", cki, " removing it... err=", err)
		cc.removeIndex(root.IndexId)
		return root, err
	}

	pos, err := cki.addInterval(root.Pos, it)
	if err != nil && err != errors2.ClosedState {
		cc.logger.Warn("error while adding data index ", cki, " removing it... err=", err)
		// considering it like corrupted. Drop the index!
		cc.removeIndex(root.IndexId)
	}
	return Item{root.IndexId, pos}, err
}

func (cc *ckiCtrlr) grEq(root Item, ts int64) (uint32, error) {
	cki := cc.getIndex(root.IndexId)
	if cki == nil {
		return 0, errors2.NotFound
	}

	r, err := cki.grEq(root.Pos, ts)
	return r.idx, err
}

func (cc *ckiCtrlr) less(root Item, ts int64) (uint32, error) {
	cki := cc.getIndex(root.IndexId)
	if cki == nil {
		return 0, errors2.NotFound
	}

	r, err := cki.less(root.Pos, ts)
	return r.idx, err
}

func (cc *ckiCtrlr) removeItem(root Item) error {
	cki := cc.getIndex(root.IndexId)
	if cki == nil || root.IndexId == 0 {
		return errors2.NotFound
	}

	return cki.deleteIndex(root.Pos)
}

// close closes all opened indexes and releases resources
func (cc *ckiCtrlr) close() error {
	cc.createLock.Lock()
	defer cc.createLock.Unlock()

	mp := cc.idxes.Load().(ckiMap)
	cc.idxes.Store(emptyMap)

	for _, cki := range mp {
		cki.Close()
	}

	return nil
}

func getListOfExistingIndexFiles(ddir string) ([]uint64, error) {
	res := make([]uint64, 0, 10)
	err := filepath.Walk(ddir, func(pth string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		ext := filepath.Ext(pth)
		if ext != idxFileExt {
			return nil
		}

		nm := info.Name()
		nm = nm[:len(nm)-len(idxFileExt)]
		sid, err := utils.ParseSimpleId(nm)
		if err != nil {
			return fmt.Errorf("found index file %s, but its name doesn't meet conventions of the tmindex files", err)
		}

		res = append(res, sid)
		return nil
	})
	return res, err
}

func (cc *ckiCtrlr) removeIndex(id uint64) {
	cc.createLock.Lock()
	defer cc.createLock.Unlock()

	cc.logger.Debug("removeIndex(): id=", id)
	// check whether if it was created
	mp := cc.idxes.Load().(ckiMap)
	if cki, ok := mp[id]; ok {
		cc.logger.Debug("removeIndex(): found id=", id, " removing it...")
		nmp := make(ckiMap)
		for k, v := range mp {
			if k != id {
				nmp[k] = v
			}
		}
		cc.idxes.Store(nmp)
		cki.Close()
		os.Remove(cc.getIndexFileName(id))
	}
}

func (cc *ckiCtrlr) getIndex(id uint64) *ckindex {
	mp := cc.idxes.Load().(ckiMap)
	return mp[id]
}

// allocateIndex works over known indexes and tries to use one for a new tree
func (cc *ckiCtrlr) allocateIndex() (uint64, *ckindex, error) {
	mp := cc.idxes.Load().(ckiMap)
	for id, cki := range mp {
		if cki.bks.Completion() < 0.9 {
			return id, cki, nil
		}
	}

	id := utils.NextSimpleId()
	cki, err := cc.createIndex(id)
	return id, cki, err
}

// extend tries to extend the underlying storage if its filled more than for 80%
func (cc *ckiCtrlr) extend(cki *ckindex) error {
	if cki.bks.Completion() > 0.8 && cki.bks.Segments() < cc.maxSegms {
		var errInt error
		err := cki.execExclusively(func() {
			sgms := cki.bks.Segments() * 2
			if sgms > cc.maxSegms {
				sgms = cc.maxSegms
			}

			ns := getStorageSize(sgms)
			bts := cki.bks.Bytes()
			cc.logger.Info("Trying to extends ckindex ", cki, " from ", cki.bks.Segments(), " segments to ", sgms, " with total size ", ns)
			errInt = bts.Grow(ns)
			if errInt == nil {
				var bks *bstorage.Blocks
				bks, errInt = bstorage.NewBlocks(blockSize, bts, false)
				if errInt == nil {
					cc.logger.Info(cki, " is extended successfully")
					cki.bks = bks
				}
			}
		})
		if err != nil {
			return err
		}
		return errInt
	}
	return nil
}

func (cc *ckiCtrlr) createIndexes(ids []uint64) error {
	cc.createLock.Lock()
	defer cc.createLock.Unlock()

	for _, id := range ids {
		_, err := cc.createIndexAndUpdateMap(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cc *ckiCtrlr) createIndex(id uint64) (*ckindex, error) {
	cc.createLock.Lock()
	defer cc.createLock.Unlock()

	// check whether if it was created
	mp := cc.idxes.Load().(ckiMap)
	if idx, ok := mp[id]; ok {
		return idx, nil
	}

	return cc.createIndexAndUpdateMap(id)
}

func (cc *ckiCtrlr) getIndexFileName(id uint64) string {
	return filepath.Join(cc.ddir, utils.SimpleId(id)+idxFileExt)
}

func (cc *ckiCtrlr) createIndexAndUpdateMap(id uint64) (*ckindex, error) {
	ifn := cc.getIndexFileName(id)

	ci, err := cc.openIndex(ifn)
	if err != nil {
		cc.logger.Warn("createIndexAndUpdateMap(): could not open the file ", ifn, " from the first attempt, err=", err)
		os.Remove(ifn)
		ci, err = cc.openIndex(ifn)
		if err != nil {
			// oops it seems like we are in deep trouble. Could not create files at all :(
			return nil, errors.Wrapf(err, "could not crate file %s from second attempt, giving up...", ifn)
		}
	}

	// ok, could create, updating the map now
	mp := cc.idxes.Load().(ckiMap)
	nmp := make(ckiMap)
	for k, v := range mp {
		nmp[k] = v
	}
	nmp[id] = ci
	cc.idxes.Store(nmp)
	return ci, nil
}

func (cc *ckiCtrlr) openIndex(ifn string) (*ckindex, error) {
	// let's start with one segment for new file
	size := getStorageSize(1)
	fi, err := os.Stat(ifn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrapf(err, "openIndex(): Could not get the file stat")
		}
		cc.logger.Info("openIndex(): seems like new file ", ifn, " will try to be created with the size ", size)
	} else {
		size = fi.Size()
	}

	// open mm file with desired size
	mmf, err := bstorage.NewMMFile(ifn, size)
	if err != nil {
		return nil, errors.Wrapf(err, "openIndex(): Could not open memory mapped file")
	}

	// open blocks on top of the mmf
	bks, err := bstorage.NewBlocks(blockSize, mmf, false)
	if err != nil {
		mmf.Close()
		return nil, errors.Wrapf(err, "openIndex(): could not create block storage on top of mmf")
	}

	cc.logger.Info("openIndex(): opened index for ", ifn, " with the size ", size)

	return newCkIndex(bks), nil
}

// getStorageSize returns the storage size based on number of segments
// and taking into account that the value must be devided on 4096 with no reminder
func getStorageSize(segms int) int64 {
	bks := bstorage.GetBlocksInSegment(blockSize)
	segmSize := int64(bks * blockSize)
	size := int64(segms) * segmSize
	if size%4096 != 0 {
		size = ((size / 4096) + 1) * 4096
	}
	return size
}

func (itm Item) String() string {
	return fmt.Sprintf("{IndexId: %s, pos: %d}", utils.SimpleId(itm.IndexId), itm.Pos)
}
