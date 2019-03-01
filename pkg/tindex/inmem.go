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
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type (
	tagsDesc struct {
		Tags tag.Set
		Src  string
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
		tmap   map[string]*tagsDesc
		done   bool
	}
)

const (
	cIdxFileName       = "tindex.dat"
	cIdxBackupFileName = "tindex.bak"
)

func NewInmemService() Service {
	ims := new(inmemService)
	ims.logger = log4g.GetLogger("tindex.inmem")
	ims.tmap = make(map[string]*tagsDesc)
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

func (ims *inmemService) GetOrCreateJournal(tags string) (string, error) {
	ims.lock.Lock()
	if ims.done {
		ims.lock.Unlock()
		return "", fmt.Errorf("Already shut-down.")
	}

	td, ok := ims.tmap[tags]
	if !ok {
		tgs, err := tag.Parse(tags)
		if err != nil {
			ims.lock.Unlock()
			return "", fmt.Errorf("The line %s doesn't seem like properly formatted tag line: %s", tags, err)
		}

		if tgs.IsEmpty() {
			return "", fmt.Errorf("At least one tag value is expected to define the source")
		}

		if td2, ok := ims.tmap[string(tgs.Line())]; !ok {
			td = &tagsDesc{tgs, newSrc()}
			ims.tmap[string(tgs.Line())] = td
			err = ims.saveStateUnsafe()
			if err != nil {
				delete(ims.tmap, string(tgs.Line()))
				ims.logger.Error("Could not save state for the new source ", td.Src, " formed for ", tgs.Line(), ", original Tags=", tags, ", err=", err)
				ims.lock.Unlock()
				return "", err
			}
		} else {
			td = td2
		}
	}

	res := td.Src
	ims.lock.Unlock()
	return res, nil
}

func (ims *inmemService) GetJournals(tagsCond string, maxSize int, checkAll bool) (map[tag.Line]string, int, error) {
	tef, err := lql.BuildTagsExpFunc(tagsCond)
	if err != nil {
		return nil, 0, err
	}

	ims.lock.Lock()
	if ims.done {
		ims.lock.Unlock()
		return nil, 0, fmt.Errorf("Already shut-down.")
	}

	count := 0
	res := make(map[tag.Line]string, 10)
	for _, td := range ims.tmap {
		if tef(td.Tags) {
			count++
			if len(res) < maxSize {
				res[td.Tags.Line()] = td.Src
			} else if !checkAll {
				break
			}
		}
	}
	ims.lock.Unlock()

	return res, count, nil
}

func (ims *inmemService) saveStateUnsafe() error {
	ims.logger.Debug("saveStateUnsafe()")
	if ims.Config.DoNotSave {
		ims.logger.Warn("Will not save config, cause DoNotSave flag is set.")
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
		return errors.Wrapf(err, "Could not rename file %s to %s", fn, bFn)
	}

	data, err := json.Marshal(ims.tmap)
	if err != nil {
		return errors.Wrapf(err, "Could not marshal tmap ")
	}

	if err = ioutil.WriteFile(fn, data, 0640); err != nil {
		return errors.Wrapf(err, "Could not write file %s ", fn)
	}

	return nil
}

func (ims *inmemService) checkConsistency(ctx context.Context) error {
	err := ims.loadState()
	if err != nil {
		return err
	}

	ims.logger.Info("Checking the index and data consistency")
	knwnJrnls := ims.Journals.GetJournals(ctx)
	fail := false
	km := make(map[string]string, len(ims.tmap))
	for _, d := range ims.tmap {
		km[d.Src] = d.Src
	}

	for _, src := range knwnJrnls {
		if _, ok := km[src]; !ok {
			ims.logger.Error("Found journal ", src, ", but it is not in the tindex")
			fail = true
			continue
		}
		delete(km, src)
	}

	if len(km) > 0 {
		ims.logger.Warn("tindex contains %d records, which don't have corresponding journals")
	}

	if fail {
		ims.logger.Error("Consistency check failed. ", len(knwnJrnls), " sources found and ", len(ims.tmap), " records in tindex")
		return errors.Errorf("Data is inconsistent. %d journals and %d tindex records found. Some journals don't have records in the tindex", len(knwnJrnls), len(ims.tmap))
	}
	ims.logger.Info("Consistency check passed. ", len(knwnJrnls), " sources found and all of them have correct tindex record. ",
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
		return errors.Wrapf(err, "Cound not load index file %s. Wrong permissions?", fn)
	}

	return json.Unmarshal(data, &ims.tmap)
}
