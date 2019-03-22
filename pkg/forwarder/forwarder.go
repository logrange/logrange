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

package forwarder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/forwarder/sink"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/mohae/deepcopy"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type (
	desc struct {
		Worker   *WorkerConfig
		Position string
		position atomic.Value
	}

	descs map[string]*desc

	workers map[string]*worker

	Forwarder struct {
		cfg *Config

		descs   atomic.Value
		workers atomic.Value
		waitWg  sync.WaitGroup

		client  api.Client
		storage storage.Storage
		logger  log4g.Logger
	}
)

const (
	storageKeyName = "forwarder.json"
)

//===================== forwarder =====================

func NewForwarder(cfg *Config, cli api.Client, storage storage.Storage) (*Forwarder, error) {
	if err := cfg.Check(); err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}

	f := new(Forwarder)
	f.cfg = deepcopy.Copy(cfg).(*Config)

	f.workers.Store(make(workers))
	f.descs.Store(make(descs))

	f.client = cli
	f.storage = storage

	f.logger = log4g.GetLogger("forwarder")
	return f, nil
}

func (f *Forwarder) Run(ctx context.Context) error {
	f.logger.Info("Running, config=", f.cfg)
	if err := f.init(ctx); err != nil {
		return err
	}
	f.runScanConfig(ctx)
	f.runPersistState(ctx)
	return nil
}

func (f *Forwarder) update(ctx context.Context) {
	nd := f.toDescs(f.cfg)
	if nd != nil {
		md := f.mergeDescs(f.getDescs(), nd)
		f.syncWorkers(ctx, md)
		f.setDescs(md)
	}
}

func (f *Forwarder) Close() error {
	var err error
	if !utils.WaitWaitGroup(&f.waitWg, time.Minute) {
		err = errors.New("close timeout")
	}
	f.logger.Info("Closed, err=", err)
	return nil
}

func (f *Forwarder) init(ctx context.Context) error {
	err := f.loadState()
	if err == nil {
		f.update(ctx)
	}
	return err
}

func (f *Forwarder) getDescs() descs {
	return f.descs.Load().(descs)
}

func (f *Forwarder) setDescs(d descs) {
	f.descs.Store(d)
}

func (f *Forwarder) toDescs(cfg *Config) descs {
	if cfg == nil || cfg.Workers == nil {
		return nil
	}
	dd := make(descs, len(cfg.Workers))
	for _, w := range cfg.Workers {
		d := new(desc)
		d.Worker = deepcopy.Copy(w).(*WorkerConfig)
		d.position.Store("")
		dd[w.Name] = d
	}
	return dd
}

//===================== forwarder.jobs =====================

func (f *Forwarder) runScanConfig(ctx context.Context) {
	f.logger.Info("Running scanning config every ", f.cfg.ConfigScanIntervalSec, " seconds...")
	ticker := time.NewTicker(time.Second *
		time.Duration(f.cfg.ConfigScanIntervalSec))

	f.waitWg.Add(1)
	go func() {
		for utils.Wait(ctx, ticker) {
			if err := f.cfg.Reload(); err != nil {
				f.logger.Warn("Failed scanning config this time, retry later, err=", err)
				continue
			}
			f.update(ctx)
		}
		f.logger.Warn("Config scan stopped.")
		f.waitWg.Done()
	}()
}

func (f *Forwarder) runPersistState(ctx context.Context) {
	f.logger.Info("Running persist state every ", f.cfg.StateStoreIntervalSec, " seconds...")
	ticker := time.NewTicker(time.Second *
		time.Duration(f.cfg.StateStoreIntervalSec))

	f.waitWg.Add(1)
	go func() {
		for utils.Wait(ctx, ticker) {
			if err := f.persistState(); err != nil {
				f.logger.Error("Unable to persist state, cause=", err)
			}
		}
		_ = f.persistState()
		f.logger.Warn("Persist state stopped.")
		f.waitWg.Done()
	}()
}

//===================== forwarder.workers =====================

func (f *Forwarder) syncWorkers(ctx context.Context, ds descs) {
	newWks := make(workers)
	oldWks := f.workers.Load().(workers)

	f.logger.Info("Syncing workers: new#=", len(ds), ", old#=", len(oldWks))
	for name, d := range ds {
		w, ok := oldWks[name]
		if ok && d != w.desc {
			w.stopGracefully() //stop replaced
		}
		var err error
		if !ok || w.isStopped() { //start new
			if w, err = f.runWorker(ctx, d); err != nil {
				f.logger.Error("Failed to run worker, desc=", d, ", err=", err)
				continue
			}
		}
		newWks[name] = w
	}
	for name, w := range oldWks { //stop deleted
		if _, ok := newWks[name]; !ok {
			if !w.isStopped() {
				w.stopGracefully()
				newWks[name] = w
			}
		}
	}
	f.workers.Store(newWks)
	f.logger.Info("Sync workers is done.")
}

func (f *Forwarder) newWorkerConfig(d *desc) (*workerConfig, error) {
	snk, err := sink.NewSink(d.Worker.Sink)
	if err != nil {
		return nil, fmt.Errorf("failed to create sink=%v, err=%v", d.Worker.Sink, err)
	}
	return &workerConfig{
		desc:   d,
		sink:   snk,
		rpcc:   f.client,
		logger: f.logger.WithId(fmt.Sprintf("[%v]", d.Worker.Name)).(log4g.Logger),
	}, nil
}

func (f *Forwarder) runWorker(ctx context.Context, d *desc) (*worker, error) {
	wcfg, err := f.newWorkerConfig(d)
	if err != nil {
		return nil, err
	}

	w := newWorker(wcfg)
	f.waitWg.Add(1)
	go func(w *worker) {
		_ = w.run(ctx)
		f.waitWg.Done()
	}(w)

	return w, nil
}

//===================== forwarder.descs =====================

func (f *Forwarder) mergeDescs(old, new descs) descs {
	f.logger.Info("Merging descriptors: new#=", len(new), ", old#=", len(old), "...")
	var a, r, d int

	res := make(descs)
	for name, nd := range new {
		od, ok := old[name]
		if !ok {
			f.logger.Debug("Merge: add=", nd)
			res[name] = nd
			a++
			continue
		}
		if reflect.DeepEqual(od, nd) {
			res[name] = od
			continue
		}
		f.logger.Debug("Merge: repl (from=", od, ", to=", nd, ")")
		res[name] = nd
		r++
	}
	for name, od := range old {
		if _, ok := res[name]; !ok {
			f.logger.Debug("Merge: del=", od)
			d++
		}
	}

	f.logger.Info("Merging result (total=", len(old)+a-d, "): add#=", a, ", repl#=", r, ", del#=", d)
	return res
}

//===================== forwarder.state =====================

func (f *Forwarder) loadState() error {
	f.logger.Info("Loading state from storage=", f.storage)
	data, err := f.storage.ReadData(storageKeyName)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	d := make(descs)
	if len(data) > 0 {
		if err = json.Unmarshal(data, &d); err != nil {
			return fmt.Errorf("cannot unmarshal state from %v; cause: %v", f.storage, err)
		}
	}
	f.setDescs(d)
	f.logger.Info("Loaded state (size=", len(data), "bytes)")
	return nil
}

func (f *Forwarder) persistState() error {
	f.logger.Info("Persisting state to storage=", f.storage)
	d := f.getDescs()

	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("cannot marshal state=%v; cause: %v", d, err)
	}

	err = f.storage.WriteData(storageKeyName, data)
	if err == nil {
		f.logger.Info("Persisted state (size=", len(data), "bytes)")
	}
	return err
}

//===================== descs =====================

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
			ds[d.Worker.Name] = d
		}
	}
	return err
}

func (ds descs) String() string {
	return utils.ToJsonStr(ds)
}

//===================== desc =====================

func (d *desc) MarshalJSON() ([]byte, error) {
	type alias desc
	return json.Marshal(&struct {
		*alias
		Position string
	}{
		alias:    (*alias)(d),
		Position: d.getPosition(),
	})
}

func (d *desc) UnmarshalJSON(data []byte) error {
	type alias desc
	t := &struct {
		alias
	}{alias: (alias)(*d)}

	err := json.Unmarshal(data, t)
	if err == nil {
		*d = desc(t.alias)
		d.setPosition(d.Position)
		return err
	}

	return err
}

func (d *desc) setPosition(val string) {
	d.position.Store(val)
}

func (d *desc) getPosition() string {
	p := d.position.Load()
	if p == nil {
		return ""
	}
	return p.(string)
}

func (d *desc) String() string {
	return utils.ToJsonStr(d)
}
