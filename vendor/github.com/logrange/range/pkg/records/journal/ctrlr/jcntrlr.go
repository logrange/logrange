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

package ctrlr

import (
	"context"
	"fmt"
	"github.com/logrange/range/pkg/utils/bytes"
	errors2 "github.com/logrange/range/pkg/utils/errors"
	"github.com/logrange/range/pkg/utils/fileutil"
	"sync"

	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/cluster/model"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	"github.com/logrange/range/pkg/records/journal"
	"github.com/pkg/errors"
)

type (
	// JournalControllerConfig interface provides
	JournalControllerConfig interface {
		// FdPoolSize returns size of chunk.FdPool
		FdPoolSize() int

		// StorageDir returns path to the dir on the local File system, where journals are stored
		StorageDir() string

		// GetChunkConfig returns chunkfs.Config object, which will be used for constructing
		// chunks
		GetChunkConfig() chunkfs.Config
	}

	jrnlController struct {
		JrnlCatalog model.JournalCatalog    `inject:""`
		JCfg        JournalControllerConfig `inject:"JournalControllerConfig"`

		fdPool *chunkfs.FdPool
		adv    *advertiser
		logger log4g.Logger
		lock   sync.Mutex
		jmap   map[string]jrnlHolder
	}

	jrnlHolder struct {
		cc   *chnksController
		jrnl journal.Journal
	}
)

func NewJournalController() journal.Controller {
	jc := new(jrnlController)
	jc.logger = log4g.GetLogger("journal.Controller")
	jc.jmap = make(map[string]jrnlHolder)
	return jc
}

// PostConstruct is a part of linker.PostConstructor interface. Will be called by
// linker.Injector due to initialization cycle.
func (jc *jrnlController) PostConstruct() {
	poolSize := 10000
	if jc.JCfg.FdPoolSize() > 0 {
		poolSize = jc.JCfg.FdPoolSize()
		jc.logger.Info("Will create FdPool with size=", poolSize)
	}
	jc.fdPool = chunkfs.NewFdPool(poolSize)
	jc.adv = newAdvertiser(jc.JrnlCatalog)
}

// Init is a part of linker.Initializer interface, it is called by linker.Injector
func (jc *jrnlController) Init(ctx context.Context) error {
	dir := jc.JCfg.StorageDir()
	jc.logger.Info("Scanning ", dir, " for journals")
	jrnls, err := scanForJournals(dir)
	if err != nil {
		jc.logger.Error("Init(): the dir ", dir, " scanning error")
		return err
	}

	jc.logger.Info(len(jrnls), " journals found, will construct them right now.")
	for _, jn := range jrnls {
		_, err := jc.createNewJournal(jn, false)
		if err != nil {
			jc.logger.Error("Could not consturct new jousrnal with name ", jn, ", skipping. err=", err)
		}
	}
	return nil
}

// GetJournals returns a slice of known journals
func (jc *jrnlController) Visit(ctx context.Context, cv journal.ControllerVisitorF) {
	//TODO we use local storage so far, but it needs to have JournalCatalog in the future
	jc.lock.Lock()
	res := make([]journal.Journal, 0, len(jc.jmap))
	for _, jh := range jc.jmap {
		res = append(res, jh.jrnl)
	}
	jc.lock.Unlock()

	for _, j := range res {
		if !cv(j) {
			break
		}
	}
}

// GetOrCreate returns journal by its name. It is part of journal.Contorller
func (jc *jrnlController) GetOrCreate(ctx context.Context, jname string) (journal.Journal, error) {
	jc.lock.Lock()
	var err error
	jh, ok := jc.jmap[jname]
	if !ok {
		jh, err = jc.createNewJournal(jname, true)
	}
	jc.lock.Unlock()
	jh.cc.ensureInit()
	return jh.jrnl, err
}

func (jc *jrnlController) Delete(ctx context.Context, jname string) error {
	jc.lock.Lock()
	jh, ok := jc.jmap[jname]
	var err error
	if !ok {
		err = errors2.NotFound
	}
	if jh.cc.isEmpty() {
		delete(jc.jmap, jname)
	} else {
		err = errors2.IsNotEmpty
	}
	jc.lock.Unlock()
	if err != nil {
		return err
	}
	jh.cc.shutdown(ctx)
	return nil
}

// createNewJournal creates new journal with name jn. It scans folder and creates new journal either there are chunks
// there or noChunksOk==true
func (jc *jrnlController) createNewJournal(jn string, noChunksOk bool) (jrnlHolder, error) {
	if !journal.NameRegExp.Match(bytes.StringToByteArray(jn)) {
		return jrnlHolder{}, fmt.Errorf("Wrong journal name=\"%s\", which doesn't match to the name pattern=\"%s\"", jn, journal.JOURNAL_NAME_REGEX)
	}

	pth, err := journalPath(jc.JCfg.StorageDir(), jn)
	if err != nil {
		return jrnlHolder{}, errors.Wrapf(err, "Could not make journal path name for journal=%s", jn)
	}
	err = fileutil.EnsureDirExists(pth)
	if err != nil {
		return jrnlHolder{}, errors.Wrapf(err, "Could not create journal path for journal=%s", jn)
	}

	fscc := newFSChnksController(jn, pth, jc.fdPool, jc.JCfg.GetChunkConfig())
	cc := newChunksController(jn, fscc, jc.adv)
	cks, _ := fscc.scan(!noChunksOk)
	if len(cks) == 0 && !noChunksOk {
		jc.logger.Info("No chunks found in ", pth, ", skipping the journal ", jn, " creating.")
		return jrnlHolder{}, nil
	}
	jh := jrnlHolder{cc, journal.New(cc)}
	jc.jmap[jn] = jh
	return jh, nil

}
