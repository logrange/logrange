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
	"fmt"
	"github.com/logrange/logrange/pkg/forwarder/sink"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/mohae/deepcopy"
	"reflect"
	"regexp/syntax"
	"strings"
)

type (
	PipeConfig struct {
		Name   string
		Source string
		Filter string
	}

	WorkerConfig struct {
		Name string
		Pipe *PipeConfig
		Sink *sink.Config
	}

	Config struct {
		Workers                []*WorkerConfig
		StateStoreIntervalSec  int
		SyncWorkersIntervalSec int
		ReloadFn               func() (*Config, error) `json:"-"`
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	return &Config{
		Workers:                []*WorkerConfig{},
		StateStoreIntervalSec:  10,
		SyncWorkersIntervalSec: 20,
	}
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	if other.StateStoreIntervalSec != 0 {
		c.StateStoreIntervalSec = other.StateStoreIntervalSec
	}
	if other.SyncWorkersIntervalSec != 0 {
		c.SyncWorkersIntervalSec = other.SyncWorkersIntervalSec
	}
	if len(other.Workers) != 0 {
		c.Workers = deepcopy.Copy(other.Workers).([]*WorkerConfig)
	}
	if other.ReloadFn != nil {
		c.ReloadFn = other.ReloadFn
	}
}

func (c *Config) Check() error {
	if c.StateStoreIntervalSec <= 0 {
		return fmt.Errorf("invalid StateStoreIntervalSec=%v, must be > 0sec", c.StateStoreIntervalSec)
	}
	if c.SyncWorkersIntervalSec <= 0 {
		return fmt.Errorf("invalid SyncWorkersIntervalSec=%v, must be > 0sec", c.SyncWorkersIntervalSec)
	}

	wNames := make(map[string]bool)
	for _, w := range c.Workers {
		if _, ok := wNames[w.Name]; ok {
			return fmt.Errorf("invalid Worker=%v: duplicate Name, must be unique", w)
		}
		wNames[w.Name] = true
		err := w.Check()
		if err != nil {
			return fmt.Errorf("invalid Worker=%v: %v", w, err)
		}
	}

	return nil
}

func (c *Config) Reload() (bool, error) {
	var (
		err error
		nc  *Config
	)
	if c.ReloadFn != nil {
		nc, err = c.ReloadFn()
		if err == nil {
			if !c.Equals(nc) {
				err = nc.Check()
				if err == nil {
					c.Apply(nc)
					return true, nil
				}
			}
		}
	}
	return false, err
}

func (c *Config) Equals(other *Config) bool {
	if other == nil {
		return false
	}

	return c.StateStoreIntervalSec == other.StateStoreIntervalSec &&
		c.SyncWorkersIntervalSec == other.SyncWorkersIntervalSec &&
		reflect.DeepEqual(c.Workers, other.Workers)
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}

//===================== workerConfig =====================

func (wc *WorkerConfig) Check() error {
	if strings.TrimSpace(wc.Name) == "" {
		return fmt.Errorf("invalid Name=%v, must be non-empty", wc.Name)
	}
	if wc.Pipe == nil {
		return fmt.Errorf("invalid Pipe=%v, must be non-nil", wc.Pipe)
	}
	if wc.Sink == nil {
		return fmt.Errorf("invalid Sink=%v, must be non-nil", wc.Sink)
	}

	err := wc.Pipe.Check()
	if err != nil {
		return fmt.Errorf("invalid Pipe=%v: %v", wc.Pipe, err)
	}
	err = wc.Sink.Check()
	if err != nil {
		return fmt.Errorf("invalid Sink=%v: %v", wc.Sink, err)
	}

	return nil
}

func (wc *WorkerConfig) String() string {
	return utils.ToJsonStr(wc)
}

//===================== streamConfig =====================

func (sc *PipeConfig) Check() error {
	if sc.Name != "" && (sc.Source != "" || sc.Filter != "") {
		return fmt.Errorf("both Source and Filter must be empty " +
			"when Name is not empty")
	}
	if sc.Name == "" {
		if _, err := lql.ParseSource(sc.Source); err != nil {
			return fmt.Errorf("invalid Source=%s: %v", sc.Source, err)
		}
		if _, err := lql.ParseExpr(sc.Filter); err != nil {
			return fmt.Errorf("invalid Filter=%s: %v", sc.Filter, err)
		}
		if sc.Filter != "" {
			if _, err := syntax.Parse(sc.Filter, syntax.Perl); err != nil {
				return fmt.Errorf("invalid Filter=%s: %v", sc.Filter, err)
			}
		}
	}
	return nil
}

func (sc *PipeConfig) String() string {
	return utils.ToJsonStr(sc)
}
