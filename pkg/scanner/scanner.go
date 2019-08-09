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

package scanner

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/logrange/logrange/pkg/scanner/parser"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/pkg/errors"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrivets/log4g"
	"github.com/mohae/deepcopy"
)

type (
	desc struct {
		Id           string
		File         string
		Offset       int64
		LastSeenSize int64
	}

	descs map[string]*desc

	workers map[string]*worker

	// Scanner struct manages data collecting jobs for sending the data into
	// Logrange server.
	Scanner struct {
		cfg *Config

		schemas  []*schema
		excludes []*regexp.Regexp

		descs       atomic.Value
		workers     atomic.Value
		workerIdCnt int32

		waitWg  sync.WaitGroup
		storage storage.Storage
		logger  log4g.Logger
	}

	Stats struct {
		Workers []*stats
	}
)

const (
	storageKeyName = "scanner.json"
)

// NewScanner creates a new file scanner according the config provided. It returns
// the pointer to Scanner object, or an error, if any.
func NewScanner(cfg *Config, storage storage.Storage) (*Scanner, error) {
	if err := cfg.Check(); err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}

	s := new(Scanner)
	s.cfg = deepcopy.Copy(cfg).(*Config)

	s.workers.Store(make(workers))
	s.descs.Store(make(descs))

	s.schemas = make([]*schema, 0, len(cfg.Schemas))
	for _, sh := range s.cfg.Schemas {
		s.schemas = append(s.schemas, newSchema(sh))
	}

	s.excludes = make([]*regexp.Regexp, 0, len(cfg.ExcludeMatchers))
	for _, ex := range s.cfg.ExcludeMatchers {
		re, _ := regexp.Compile(ex)
		s.excludes = append(s.excludes, re)
	}

	s.storage = storage
	s.logger = log4g.GetLogger("scanner")
	return s, nil
}

// Run starts internal go routines which will scan folders and send the obtained data asynchronously
func (s *Scanner) Run(ctx context.Context, events chan<- *model.Event) error {
	s.logger.Info("Running, config=", s.cfg)
	if err := s.init(ctx, events); err != nil {
		return err
	}

	s.runSyncWorkers(ctx, events)
	s.runPersistState(ctx)
	return nil
}

// WaitAllJobsDone closes the Scanner object
func (s *Scanner) WaitAllJobsDone() error {
	var err error
	if !utils.WaitWaitGroup(&s.waitWg, time.Minute) {
		err = errors.New("close timeout")
	}
	s.logger.Info("Closed, err=", err)
	return nil
}

func (s *Scanner) init(ctx context.Context, events chan<- *model.Event) error {
	err := s.loadState()
	if err == nil {
		s.sync(ctx, events)
	}
	return err
}

func (s *Scanner) sync(ctx context.Context, events chan<- *model.Event) {
	nd := s.scanPaths()
	md := s.mergeDescs(s.getDescs(), nd)
	s.syncWorkers(ctx, md, events)
	s.setDescs(md)
}

func (s *Scanner) getDescs() descs {
	return s.descs.Load().(descs)
}

func (s *Scanner) setDescs(d descs) {
	s.descs.Store(d)
}

func (s *Scanner) runSyncWorkers(ctx context.Context, events chan<- *model.Event) {
	s.logger.Info("Running sync workers every ", s.cfg.SyncWorkersIntervalSec, " seconds...")
	ticker := time.NewTicker(time.Second *
		time.Duration(s.cfg.SyncWorkersIntervalSec))

	s.waitWg.Add(1)
	go func() {
		// scan folders periodically until ctx is closed
		for utils.Wait(ctx, ticker) {
			s.sync(ctx, events)
		}

		ticker.Stop()
		s.logger.Warn("Sync workers stopped.")
		s.waitWg.Done()
	}()
}

func (s *Scanner) runPersistState(ctx context.Context) {
	s.logger.Info("Running persist state every ", s.cfg.StateStoreIntervalSec, " seconds...")
	ticker := time.NewTicker(time.Second *
		time.Duration(s.cfg.StateStoreIntervalSec))

	s.waitWg.Add(1)
	go func() {
		for utils.Wait(ctx, ticker) {
			if err := s.persistState(); err != nil {
				s.logger.Error("Unable to persist state, cause=", err)
			}
		}
		_ = s.persistState()
		s.logger.Warn("Persist state stopped.")
		s.waitWg.Done()
	}()
}

func (s *Scanner) syncWorkers(ctx context.Context, ds descs, events chan<- *model.Event) {
	newWks := make(workers)
	oldWks := s.workers.Load().(workers)

	s.logger.Info("Syncing workers: new#=", len(ds), ", old#=", len(oldWks))
	for id, d := range ds {
		w, ok := oldWks[id]
		if ok && d != w.desc {
			w.stopOnEOF() //stop rotated (old)
		}
		var err error
		if !ok || w.isStopped() { //start new and rotated (new)
			if w, err = s.runWorker(ctx, d, events); err != nil {
				s.logger.Error("Failed to run worker, desc=", d, ", err=", err)
				continue
			}
		}
		newWks[id] = w
	}

	//stop deleted
	for id, w := range oldWks {
		if _, ok := newWks[id]; !ok {
			if !w.isStopped() {
				w.stopOnEOF()
				newWks[id] = w
			}
		}
	}

	s.workers.Store(newWks)
	s.logger.Info("Sync workers is done.")
}

func (s *Scanner) newWorkerConfig(d *desc) (*workerConfig, error) {
	shm := s.getSchema(d)
	if shm == nil {
		return nil, errors.New("no schema found...")
	}

	p, err := parser.NewParser(&parser.Config{
		File:            d.File,
		MaxRecSizeBytes: s.cfg.RecordMaxSizeBytes,
		DataFmt:         shm.cfg.DataFormat,
		DateFmts:        shm.cfg.DateFormats,
		FieldMap:        shm.getMeta(d).Fields,
	})
	if err != nil {
		return nil, err
	}

	id := int(atomic.AddInt32(&s.workerIdCnt, 1))
	return &workerConfig{
		desc:         d,
		schema:       shm,
		recsPerEvent: s.cfg.EventMaxRecords,
		parser:       p,
		logger:       s.logger.WithId(fmt.Sprintf("[%v]", id)).(log4g.Logger),
	}, nil
}

func (s *Scanner) runWorker(ctx context.Context, d *desc, events chan<- *model.Event) (*worker, error) {
	wcfg, err := s.newWorkerConfig(d)
	if err != nil {
		return nil, err
	}

	w := newWorker(wcfg)
	s.waitWg.Add(1)
	go func(w *worker) {
		_ = w.run(ctx, events)
		s.waitWg.Done()
	}(w)

	return w, nil
}

func (s *Scanner) getExcludeRe(f string) *regexp.Regexp {
	for _, re := range s.excludes {
		if re.Match([]byte(f)) {
			return re
		}
	}
	return nil
}

func (s *Scanner) getSchema(d *desc) *schema {
	for _, s := range s.schemas {
		if s.matcher.MatchString(d.File) {
			return s
		}
	}
	return nil
}

func (s *Scanner) mergeDescs(old, new descs) descs {
	s.logger.Info("Merging descriptors: new#=", len(new), ", old#=", len(old), "...")
	var a, r, d int

	res := make(descs)
	for id, nd := range new {
		od, ok := old[id]
		if !ok {
			s.logger.Debug("Merge: add=", nd)
			res[id] = nd
			a++
			continue
		}
		if od.LastSeenSize <= nd.LastSeenSize && od.getOffset() <= nd.LastSeenSize {
			od.setLastSeenSize(nd.getLastSeenSize())
			res[id] = od
			continue
		}
		s.logger.Debug("Merge: repl (from=", od, ", to=", nd, ")")
		res[id] = nd
		r++
	}
	for id, od := range old {
		if _, ok := res[id]; !ok {
			s.logger.Debug("Merge: del=", od)
			d++
		}
	}

	s.logger.Info("Merging result (total=", len(old)+a-d, "): add#=", a, ", repl#=", r, ", del#=", d)
	return res
}

func (s *Scanner) scanPaths() descs {
	s.logger.Info("Scanning paths, includes=", s.cfg.IncludePaths,
		", excludes=", s.cfg.ExcludeMatchers, "...")

	files := s.getFilesToScan(s.cfg.IncludePaths)
	s.logger.Info("Found ", len(files), " files=", utils.ToJsonStr(files))

	res := make(descs)
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			s.logger.Warn("Skipping file=", f, ", unable to get info for it; cause: ", err)
			continue
		}
		re := s.getExcludeRe(f)
		if re != nil {
			s.logger.Warn("Skipping file=", f, ", it is excluded with regExp=", re.String())
			continue
		}
		id := utils.GetFileId(f, info)
		res[id] = &desc{Id: id, File: f, Offset: 0, LastSeenSize: info.Size()}
	}
	return res
}

func (s *Scanner) getFilesToScan(paths []string) []string {
	ep := utils.ExpandPaths(paths)
	ff := make([]string, 0, len(ep))
	for _, p := range ep {
		var err error
		fInf, err := os.Stat(p)
		if err != nil {
			s.logger.Warn("Skipping path=", p, "; cause: ", err)
			continue
		}
		if fInf.IsDir() {
			s.logger.Warn("Skipping path=", p, "; cause: the path is directory")
			continue
		}
		ff = append(ff, p)
	}
	return utils.RemoveDups(ff)
}

func (s *Scanner) loadState() error {
	s.logger.Info("Loading state from storage=", s.storage)
	data, err := s.storage.ReadData(storageKeyName)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	d := make(descs)
	if len(data) > 0 {
		if err = json.Unmarshal(data, &d); err != nil {
			return fmt.Errorf("cannot unmarshal state from %v; cause: %v", s.storage, err)
		}
	}
	s.setDescs(d)
	s.logger.Info("Loaded state (size=", len(data), "bytes)")
	return nil
}

func (s *Scanner) persistState() error {
	s.logger.Info("Persisting state to storage=", s.storage)
	d := s.getDescs()

	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("cannot marshal state=%v; cause: %v", d, err)
	}

	err = s.storage.WriteData(storageKeyName, data)
	if err == nil {
		s.logger.Info("Persisted state (size=", len(data), "bytes)")
	}
	return err
}

func (ds descs) MarshalJSON() ([]byte, error) {
	dl := make([]*desc, 0, len(ds))
	for _, d := range ds {
		dl = append(dl, d)
	}
	return json.Marshal(&dl)
}

func (ds descs) UnmarshalJSON(data []byte) error {
	dl := make([]*desc, 0, 5)
	err := json.Unmarshal(data, &dl)
	if err == nil {
		for _, d := range dl {
			ds[d.Id] = d
		}
	}
	return err
}

func (ds descs) String() string {
	return utils.ToJsonStr(ds)
}

func (d *desc) MarshalJSON() ([]byte, error) {
	type alias desc
	return json.Marshal(&struct {
		*alias
		Offset       int64
		LastSeenSize int64
	}{
		alias:        (*alias)(d),
		Offset:       d.getOffset(),
		LastSeenSize: d.getLastSeenSize(),
	})
}

func (d *desc) addOffset(val int64) {
	atomic.AddInt64(&d.Offset, val)
}

func (d *desc) setOffset(val int64) {
	atomic.StoreInt64(&d.Offset, val)
}

func (d *desc) getOffset() int64 {
	return atomic.LoadInt64(&d.Offset)
}

func (d *desc) setLastSeenSize(val int64) {
	atomic.StoreInt64(&d.LastSeenSize, val)
}

func (d *desc) getLastSeenSize() int64 {
	return atomic.LoadInt64(&d.LastSeenSize)
}

func (d *desc) String() string {
	return utils.ToJsonStr(d)
}
