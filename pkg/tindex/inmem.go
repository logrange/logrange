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

package tindex

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type (
	tagsDesc struct {
		tags model.Tags
		src  string
	}

	// InMemConfig struct contains configuration for inmemService
	InMemConfig struct {
		// DoNotSave flag indicates that the data should not be persisted. Used for testing.
		DoNotSave bool

		// WorkingDir contains path to the folder for persisting the index data
		WorkingDir string

		// CreateNew flag indicates that if there is no index, then new one should be created, rather than reporting
		// an error on the initialization stage
		CreateNew bool
	}

	inmemService struct {
		Config *InMemConfig `inject:"inmemServiceConfig"`

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
	return ims.loadState()
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
		tgs, err := model.NewTags(tags)
		if err != nil {
			ims.lock.Unlock()
			return "", fmt.Errorf("The line %s doesn't seem like properly formatted tag line: %s", tags, err)
		}

		if len(tgs.GetTagMap()) == 0 {
			return "", fmt.Errorf("At least one tag value is expected to define the source")
		}

		if td2, ok := ims.tmap[string(tgs.GetTagLine())]; !ok {
			td = &tagsDesc{tgs, newSrc()}
			ims.tmap[string(tgs.GetTagLine())] = td
			err = ims.saveState()
			if err != nil {
				delete(ims.tmap, string(tgs.GetTagLine()))
				ims.logger.Error("Could not save state for the new source ", td.src, " formed for ", tgs.GetTagLine(), ", original tags=", tags, ", err=", err)
				ims.lock.Unlock()
				return "", err
			}
		} else {
			td = td2
		}
	}

	res := td.src
	ims.lock.Unlock()
	return res, nil
}

func (ims *inmemService) GetJournals(exp *lql.Expression, maxSize int, checkAll bool) (map[model.TagLine]string, int, error) {
	tef, err := lql.BuildTagsExpFunc(exp)
	if err != nil {
		return nil, 0, err
	}

	ims.lock.Lock()
	if ims.done {
		ims.lock.Unlock()
		return nil, 0, fmt.Errorf("Already shut-down.")
	}

	count := 0
	res := make(map[model.TagLine]string, 10)
	for _, td := range ims.tmap {
		if tef(td.tags.GetTagMap()) {
			count++
			if len(res) < maxSize {
				res[td.tags.GetTagLine()] = td.src
			} else if !checkAll {
				break
			}
		}
	}
	ims.lock.Unlock()

	return res, count, nil
}

func (ims *inmemService) saveState() error {
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

func (ims *inmemService) loadState() error {
	fn := path.Join(ims.Config.WorkingDir, cIdxFileName)
	_, err := os.Stat(fn)
	if os.IsNotExist(err) {
		if !ims.Config.CreateNew {
			return errors.Wrapf(err, "Could not find index file %s", fn)
		}
		return ims.saveState()
	}

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return errors.Wrapf(err, "Cound not load index file %s. Wrong permissions?", fn)
	}

	return json.Unmarshal(data, &ims.tmap)
}
