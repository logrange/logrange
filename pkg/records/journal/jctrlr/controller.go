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
	"sync"

	"github.com/jrivets/log4g"
	lcontext "github.com/logrange/logrange/pkg/context"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/records/chunk/chunkfs"
	"github.com/logrange/logrange/pkg/records/journal"
	"github.com/logrange/logrange/pkg/util"
)

type (
	// Config contains settings for new Controller
	Config struct {
		// BaseDir contains the directory where database is stored
		BaseDir string

		// DefChunkConfig contains settings for default chunk config. Some
		// fields (like id, name etc.) will be overwritten when new chunk is
		// created
		DefChunkConfig chunkfs.Config

		// FdPoolSize defines how big the pool for file descriptors is going to be
		FdPoolSize int
	}

	// Controller struct allows to scan provided folder on a file-system,
	// collects information about possible logrange database and manages
	// found journals via its interface
	//
	// The Controller implements journal.Controller and journal.ChunksController
	Controller struct {
		cfg    Config
		logger log4g.Logger
		fdPool *chunkfs.FdPool

		lock     sync.Mutex
		closeCh  chan struct{}
		journals map[string]*jDescriptor
	}
)

func New(cfg Config) *Controller {
	c := new(Controller)
	c.journals = make(map[string]*jDescriptor)
	c.logger = log4g.GetLogger("jctrlr")
	c.closeCh = make(chan struct{})
	c.fdPool = chunkfs.NewFdPool(cfg.FdPoolSize)
	return c
}

// Scan walks through the file system for scanning journals there. Will update
// existing journals if new chunks were found.
func (c *Controller) Scan(rmOk bool) error {
	s := &scanner{log4g.GetLogger("jctrlr.scanner")}
	jrnls, err := s.scan(c.cfg.BaseDir)
	if err != nil {
		return err
	}

	c.logger.Info("Scan(): ", len(jrnls), " journals found, will apply to the descriptors")

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.isClosed() {
		return util.ErrWrongState
	}

	if rmOk {
		c.dropRemovedJournals(jrnls)
	}
	for _, j := range jrnls {
		jd, ok := c.journals[j.name]
		if !ok {
			jd = newJDescriptor(c, j)
			c.journals[j.name] = jd
		}
		jd.applyChunkIds(j.chunks)
	}

	return nil
}

func (c *Controller) dropRemovedJournals(jrnls []scJournal) {
	m := make(map[string]bool, len(jrnls))
	for _, j := range jrnls {
		m[j.name] = true
	}

	toRemove := make([]*jDescriptor, 0, len(c.journals))
	for jn, jd := range c.journals {
		if _, ok := m[jn]; ok {
			continue
		}
		toRemove = append(toRemove, jd)
	}

	if len(toRemove) == 0 {
		return
	}

	c.logger.Info("Found ", len(toRemove), " journals with no disk data. Going to remove the controller...")
	go func() {
		for _, jd := range toRemove {
			jd.Close()
		}
		c.logger.Info("Done with the empty journals")
	}()
}

func (c *Controller) Close() error {
	c.lock.Lock()
	if c.journals == nil {
		c.logger.Error("Call Close() for already closed controller")
		c.lock.Unlock()
		return util.ErrWrongState
	}

	jrnls := c.journals
	close(c.closeCh)
	c.journals = nil
	c.lock.Unlock()

	c.logger.Info("Closing controller, ", len(jrnls), " journals will be closed as well.")
	for _, jd := range jrnls {
		jd.Close()
	}
	c.logger.Info("Close(): Done.")

	return nil
}

// ----------------------- journal.Controller -------------------------------
func (c *Controller) GetOrCreate(ctx context.Context, jname string) (journal.Journal, error) {
	c.lock.Lock()
	jd, ok := c.journals[jname]
	if !ok {
		c.logger.Info("No ", jname, " journal, creating new descriptor.")
		dir, err := journalPath(c.cfg.BaseDir, jname)
		if err != nil {
			c.logger.Error("Could not make the name directory for the jname=", jname, ", err=", err)
			c.lock.Unlock()
			return nil, err
		}

		err = ensureDirExists(dir)
		if err != nil {
			c.logger.Error("Could not create the directorey ", dir, " for journal ", jname, ", err=", err)
			c.lock.Unlock()
			return nil, err
		}
		jd = newJDescriptor(c, initScJournal(jname, dir))
		c.journals[jname] = jd
	}
	c.lock.Unlock()

	return jd.getOrCreateJournal(ctx)
}

// --------------------- journal.ChunksController ----------------------------
func (c *Controller) GetChunks(j journal.Journal) chunk.Chunks {
	c.lock.Lock()
	defer c.lock.Unlock()

	jd, ok := c.journals[j.Name()]
	if !ok {
		c.logger.Error("Ascking chunks for unknown journal=", j)
		return nil
	}

	return jd.chunks.Load().(chunk.Chunks)
}

// ------------------------- jDescCtrlr interface ----------------------------
func (c *Controller) newChunkById(jd *jDescriptor, cid chunk.Id) (chunk.Chunk, error) {
	cfg := c.cfg.DefChunkConfig
	cfg.Id = chunk.NewId()
	cfg.FileName = chunkfs.MakeChunkFileName(jd.dir, cid)
	return chunkfs.New(lcontext.WrapChannel(c.closeCh), cfg, c.fdPool)
}

func (c *Controller) newJournal(jd *jDescriptor) journal.Journal {
	return journal.New(c, jd.name)
}

func (c *Controller) onChunksCreated(jd *jDescriptor) {
	c.logger.Debug("onChunksCreated(): ", jd)
}

func (c *Controller) NewChunk(ctx context.Context, j journal.Journal) (chunk.Chunks, error) {
	lstnr, ok := j.(chunk.Listener)
	if !ok {
		c.logger.Warn("The journal implementation doesn't support chunk.Listener: ", j, " no chunk notifications will be there")
		lstnr = nil
	}

	jdir, err := journalPath(c.cfg.BaseDir, j.Name())
	if err != nil {
		c.logger.Error("Could not compose or create the journal path, err=", err)
		return nil, err
	}
	cfg := c.cfg.DefChunkConfig
	cfg.Id = chunk.NewId()
	cfg.FileName = chunkfs.MakeChunkFileName(jdir, cfg.Id)
	// new chunk, so disabling check
	cfg.CheckDisabled = true

	chnk, err := chunkfs.New(ctx, cfg, c.fdPool)
	if err != nil {
		return nil, err
	}

	if lstnr != nil {
		chnk.AddListener(lstnr)
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.isClosed() {
		chnk.Close()
		return nil, util.ErrWrongState
	}

	jd := c.journals[j.Name()]
	jd.appendNewChunk(chnk)
	return jd.chunks.Load().(chunk.Chunks), nil
}

func (c *Controller) isClosed() bool {
	return c.journals == nil
}
