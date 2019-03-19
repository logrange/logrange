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

package tindex

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records/journal"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"
)

type (
	tagsDesc struct {
		tags      tag.Set
		exclusive bool
		readers   int
		// Src contains the journal source. It is capitalized to be marshaled/unmarshaled properly
		Src string
	}

	// InMemConfig struct contains configuration for inmemService
	InMemConfig struct {
		// DoNotSave flag indicates that the data should not be persisted. Used for testing.
		DoNotSave bool

		// WorkingDir contains path to the folder for persisting the index data
		WorkingDir string
	}

	inmemService struct {
		Config   *InMemConfig       `inject:"inmemServiceConfig"`
		Journals journal.Controller `inject:""`

		logger log4g.Logger
		lock   sync.Mutex
		// tmap contains tags:tagsDesc key-value pairs
		tmap map[tag.Line]*tagsDesc
		// smap contains src:tagsDesc key-value pairs
		smap map[string]*tagsDesc
		done bool
	}
)

const (
	cIdxFileName       = "tindex.dat"
	cIdxBackupFileName = "tindex.bak"
)

func NewInmemService() Service {
	ims := new(inmemService)
	ims.logger = log4g.GetLogger("tindex.inmem")
	ims.tmap = make(map[tag.Line]*tagsDesc)
	ims.smap = make(map[string]*tagsDesc)
	return ims
}

func NewInmemServiceWithConfig(cfg InMemConfig) Service {
	res := NewInmemService().(*inmemService)
	res.Config = &cfg
	return res
}

func (ims *inmemService) Init(ctx context.Context) error {
	ims.logger.Info("Initializing...")
	ims.done = false
	return ims.checkConsistency(ctx)
}

func (ims *inmemService) Shutdown() {
	ims.logger.Info("Shutting down")

	ims.lock.Lock()
	defer ims.lock.Unlock()

	ims.done = true
}

func (ims *inmemService) GetOrCreateJournal(tags string) (res string, err error) {
	for {
		ims.lock.Lock()
		if ims.done {
			ims.lock.Unlock()
			return "", fmt.Errorf("already shut-down.")
		}

		td, ok := ims.tmap[tag.Line(tags)]
		if !ok {
			tgs, err := tag.Parse(tags)
			if err != nil {
				ims.lock.Unlock()
				return "", fmt.Errorf("the line %s doesn't seem like properly formatted tag line: %s", tags, err)
			}

			if tgs.IsEmpty() {
				ims.lock.Unlock()
				return "", fmt.Errorf("at least one tag value is expected to define the source")
			}

			if td2, ok := ims.tmap[tgs.Line()]; !ok {
				td = new(tagsDesc)
				td.tags = tgs
				td.Src = newSrc()
				ims.tmap[tgs.Line()] = td
				ims.smap[td.Src] = td
				err = ims.saveStateUnsafe()
				if err != nil {
					delete(ims.tmap, tgs.Line())
					delete(ims.smap, td.Src)
					ims.logger.Error("could not save state for the new source ", td.Src, " formed for ", tgs.Line(), ", original Tags=", tags, ", err=", err)
					ims.lock.Unlock()
					return "", err
				}
			} else {
				td = td2
			}
		}

		res = td.Src
		locked := !td.exclusive
		if locked {
			td.readers++
		}
		ims.lock.Unlock()

		if locked {
			break
		}
		ims.logger.Debug("Oops, raise with an exclusive lock")
		time.Sleep(time.Millisecond)
	}
	return res, err
}

func (ims *inmemService) Visit(srcCond *lql.Source, vf VisitorF, visitFlags int) error {
	tef, err := lql.BuildTagsExpFuncBySource(srcCond)
	if err != nil {
		return err
	}

	if visitFlags&VF_SKIP_IF_LOCKED != 0 {
		return ims.visitSkippingIfLocked(tef, vf, visitFlags)
	}
	return ims.visitWaitingIfLocked(tef, vf, visitFlags)
}

func (ims *inmemService) visitSkippingIfLocked(tef lql.TagsExpFunc, vf VisitorF, visitFlags int) error {
	ims.lock.Lock()
	if ims.done {
		ims.lock.Unlock()
		return fmt.Errorf("already shut-down.")
	}

	vstd := make([]*tagsDesc, 0, 100)
	for _, td := range ims.tmap {
		if tef(td.tags) {
			if !td.exclusive {
				td.readers++
				vstd = append(vstd, td)
			}
		}
	}
	ims.lock.Unlock()

	startIdx := 0
	for i, v := range vstd {
		startIdx = i + 1
		if !vf(v.tags, v.Src) {
			break
		}
	}

	if visitFlags&VF_DO_NOT_RELEASE == 0 {
		startIdx = 0
	}

	ims.lock.Lock()
	for i := startIdx; i < len(vstd); i++ {
		vstd[i].readers--
	}
	ims.lock.Unlock()

	return nil
}

func (ims *inmemService) visitWaitingIfLocked(tef lql.TagsExpFunc, vf VisitorF, visitFlags int) error {
	ims.lock.Lock()
	if ims.done {
		ims.lock.Unlock()
		return fmt.Errorf("already shut-down.")
	}

	vstd := make([]*tagsDesc, 0, 100)
	for _, td := range ims.tmap {
		if tef(td.tags) {
			if !td.exclusive {
				vstd = append(vstd, td)
			}
		}
	}
	ims.lock.Unlock()

L1:
	for i, v := range vstd {
		skip := true
		for skip {
			ims.lock.Lock()
			if ims.done {
				ims.lock.Unlock()
				ims.logger.Warn("visitWaitingIfLocked Oops, the component was closed.")
				return errors2.WrongState
			}

			if _, ok := ims.smap[v.Src]; !ok {
				ims.logger.Debug("the journal ", v.Src, ", seems to be removed while visiting. SKipping it.")
				ims.lock.Unlock()
				vstd[i] = nil
				continue L1
			}
			skip = v.exclusive
			if !skip {
				v.readers++
			}
			ims.lock.Unlock()

			if skip {
				// Crap. We do this stupid thing here, just because it has to happen extremely rear,
				// but should be considered to do something else then
				time.Sleep(time.Millisecond)
			}
		}

		cont := vf(v.tags, v.Src)
		if visitFlags&VF_DO_NOT_RELEASE != 0 {
			vstd[i] = nil
		}
		if !cont {
			break
		}
	}

	ims.lock.Lock()
	for _, v := range vstd {
		if v != nil {
			v.readers--
		}
	}
	ims.lock.Unlock()

	return nil
}

// Release allows to release the journal name which could be acquired by GetOrCreateJournal
func (ims *inmemService) Release(jn string) {
	ims.lock.Lock()
	if td, ok := ims.smap[string(jn)]; ok {
		if td.exclusive {
			panic("Could not release the lock, which was locked exclusively " + td.String())
		} else if td.readers <= 0 {
			panic("Could not release journal, it was not acquired " + td.String())
		} else {
			td.readers--
		}
	}
	ims.lock.Unlock()
}

func (ims *inmemService) LockExclusively(jn string) bool {
	ims.lock.Lock()
	td, ok := ims.smap[jn]
	res := false
	if ok {
		if !td.exclusive && td.readers == 1 {
			td.exclusive = true
			res = true
		}
	}
	ims.lock.Unlock()
	return res
}

func (ims *inmemService) UnlockExclusively(jn string) {
	ims.lock.Lock()
	if td, ok := ims.smap[string(jn)]; ok {
		if !td.exclusive || td.readers != 1 {
			panic("Could not UnlockExclusively the lock, which was not locked exclusively " + td.String())
		} else {
			td.exclusive = false
		}
	}
	ims.lock.Unlock()
}

// Delete allows to release and delete the journal name (and its tags association). If an error
// is returned, the jn must be released via Release method anyway
func (ims *inmemService) Delete(jn string) error {
	ims.lock.Lock()
	err := errors2.NotFound
	if td, ok := ims.smap[jn]; ok {
		err = errors2.WrongState
		if td.exclusive {
			delete(ims.tmap, td.tags.Line())
			delete(ims.smap, td.Src)
			err = nil
			ims.saveStateUnsafe()
		}
	}
	ims.lock.Unlock()
	return err
}

func (ims *inmemService) saveStateUnsafe() error {
	ims.logger.Debug("saveStateUnsafe()")
	if ims.Config.DoNotSave {
		ims.logger.Warn("will not save config, cause DoNotSave flag is set.")
		return nil
	}

	fn := path.Join(ims.Config.WorkingDir, cIdxFileName)
	_, err := os.Stat(fn)
	var bFn string
	if !os.IsNotExist(err) {
		bFn = path.Join(ims.Config.WorkingDir, cIdxBackupFileName)
		err = os.Rename(fn, bFn)
	} else {
		err = nil
	}

	if err != nil {
		return errors.Wrapf(err, "could not rename file %s to %s", fn, bFn)
	}

	data, err := json.Marshal(ims.tmap)
	if err != nil {
		return errors.Wrapf(err, "could not marshal tmap ")
	}

	if err = ioutil.WriteFile(fn, data, 0640); err != nil {
		return errors.Wrapf(err, "could not write file %s ", fn)
	}

	return nil
}

func (ims *inmemService) checkConsistency(ctx context.Context) error {
	err := ims.loadState()
	if err != nil {
		return err
	}

	ims.logger.Info("Checking the index and data consistency")
	fail := false
	km := make(map[string]string, len(ims.tmap))
	for _, d := range ims.tmap {
		km[d.Src] = d.Src
	}

	jCnt := 0
	ims.Journals.Visit(ctx, func(j journal.Journal) bool {
		jCnt++
		if _, ok := km[j.Name()]; !ok {
			ims.logger.Error("found journal ", j.Name(), ", but it is not in the tindex")
			fail = true
		} else {
			delete(km, j.Name())
		}
		return true
	})

	if len(km) > 0 {
		ims.logger.Warn("tindex contains %d records, which don't have corresponding journals")
	}

	if fail {
		ims.logger.Error("Consistency check failed. ", jCnt, " sources found and ", len(ims.tmap), " records in tindex")
		return errors.Errorf("data is inconsistent. %d journals and %d tindex records found. Some journals don't have records in the tindex", jCnt, len(ims.tmap))
	}
	ims.logger.Info("Consistency check passed. ", jCnt, " sources found and all of them have correct tindex record. ",
		len(ims.tmap), " index records in total.")
	return ims.saveStateUnsafe()
}

func (ims *inmemService) loadState() error {
	fn := path.Join(ims.Config.WorkingDir, cIdxFileName)
	_, err := os.Stat(fn)
	if os.IsNotExist(err) {
		ims.logger.Warn("loadState() file not found ", fn)
		return nil
	}
	ims.logger.Debug("loadState() from ", fn)

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return errors.Wrapf(err, "cound not load index file %s. Wrong permissions?", fn)
	}

	err = json.Unmarshal(data, &ims.tmap)
	if err == nil {
		for tln, td := range ims.tmap {
			td.tags, err = tag.Parse(string(tln))
			if err != nil {
				ims.logger.Error("Could not parse tags ", tln, " which read from the index file")
				break
			}
			ims.smap[td.Src] = td
		}
	}

	return err
}

func (td *tagsDesc) String() string {
	return fmt.Sprintf("{tags=%s, exclusive=%t, readers=%d, Src=%s}", td.tags.Line(), td.exclusive, td.readers, td.Src)
}
