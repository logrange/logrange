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
	"context"
	"encoding/json"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/range/pkg/records/chunk"
	"github.com/pkg/errors"
	"io/ioutil"
	"math"
	"path"
	"sync"
)

type (
	// cindex struct allows to keep information about known chunks
	cindex struct {
		lock       sync.Mutex
		journals   map[string]sortedChunks
		logger     log4g.Logger
		dtFileName string
	}

	sortedChunks []*chkInfo

	chkInfo struct {
		Id    chunk.Id
		MinTs uint64
		MaxTs uint64
	}
)

const cIndexFileName = "cindex.dat"

func newCIndex() *cindex {
	ci := new(cindex)
	ci.logger = log4g.GetLogger("cindex")
	ci.journals = make(map[string]sortedChunks)
	return ci
}

// init allows to initialize the cindex. ddir is a folder where the cindex data is stored
func (ci *cindex) init(ddir string) {
	if len(ddir) == 0 {
		ci.logger.Warn("Empty dir provided, will not persist.")
		return
	}
	ci.dtFileName = path.Join(ddir, cIndexFileName)
	ci.logger.Info("Set data file to ", ci.dtFileName)
	ci.loadDataFromFile()
}

// onWrite updates the partition cindex. It receives the partition name src, number of records in the
// chunk and the cinfo
func (ci *cindex) onWrite(src string, firstRec, lastRec uint32, cinfo chkInfo) {
	ci.lock.Lock()
	sc, ok := ci.journals[src]
	if !ok {
		sc = make(sortedChunks, 1)
		ci.journals[src] = sc
		sc[0] = &cinfo
	}

	if !sc[len(sc)-1].update(&cinfo) {
		sc = append(sc, &cinfo)
		ci.journals[src] = sc
	}
	ci.lock.Unlock()
}

// getLastChunkInfo returns the last chunk or nil, if no data or the src is not found
func (ci *cindex) getLastChunkInfo(src string) *chkInfo {
	var res *chkInfo
	ci.lock.Lock()
	if sc, ok := ci.journals[src]; ok && len(sc) > 0 {
		res = sc[len(sc)-1]
	}
	ci.lock.Unlock()
	return res
}

// syncChunks receives a list of chunks for a partition src and updates the cindex information by the data from the chunks
func (ci *cindex) syncChunks(ctx context.Context, src string, cks chunk.Chunks) sortedChunks {
	newSC := chunksToSortedChunks(cks)
	ci.lock.Lock()
	if sc, ok := ci.journals[src]; ok {
		// re-assignment cause apply can re-allocate original slice
		newSC = newSC.apply(sc)
	}
	ci.lock.Unlock()

	ci.lightFill(ctx, cks, newSC)
	ci.lock.Lock()
	sc := ci.journals[src]
	// apply can re-allocate newSC
	newSC = newSC.apply(sc)
	ci.journals[src] = newSC
	res := newSC.makeCopy()
	ci.lock.Unlock()
	return res
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

		ts1, err := ci.getRecordTimestamp(ctx, it)
		if err != nil {
			it.Close()
			ci.logger.Warn("lightFill(): Could not read first record, err=", err)
			continue
		}

		it.SetPos(chk.Count() - 1)
		ts2, err := ci.getRecordTimestamp(ctx, it)
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

func (ci *cindex) getRecordTimestamp(ctx context.Context, it chunk.Iterator) (uint64, error) {
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

// apply overwrite value from sc1 if the existing one is nil or empty
func (sc sortedChunks) apply(sc1 sortedChunks) sortedChunks {
	i := 0
	j := 0
	for i < len(sc) && j < len(sc1) {
		if sc[i].Id > sc1[j].Id {
			j++
			continue
		}
		if sc[i].Id < sc1[j].Id {
			i++
			continue
		}
		sc[i].update(sc1[j])
		i++
		j++
	}

	// add all tailed chunks that are not in sc to sc
	for ; j < len(sc1); j++ {
		ci := new(chkInfo)
		*ci = *sc1[j]
		sc = append(sc, ci)
	}
	// returns sc cause it could re-allocate original slice
	return sc
}

func (sc sortedChunks) makeCopy() sortedChunks {
	res := make(sortedChunks, len(sc))
	for i, c := range sc {
		c1 := new(chkInfo)
		*c1 = *c
		res[i] = c1
	}
	return res
}

func (ci *chkInfo) update(another *chkInfo) bool {
	if ci.Id != another.Id {
		return false
	}

	if ci.MinTs > another.MinTs {
		ci.MinTs = another.MinTs
	}
	if ci.MaxTs < another.MaxTs {
		ci.MaxTs = another.MaxTs
	}
	return true
}
