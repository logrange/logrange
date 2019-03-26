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
	"regexp/syntax"
	"strings"
)

type (
	SourceConfig struct {
		Lql    string
		Filter string
	}

	WorkerConfig struct {
		Name   string
		Source *SourceConfig
		Sink   *sink.Config
	}

	Config struct {
		Workers                 []*WorkerConfig
		StateStoreIntervalSec   int
		ConfigReloadIntervalSec int
		ReloadFn                func() (*Config, error) `json:"-"`
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	return &Config{
		Workers:                 []*WorkerConfig{},
		StateStoreIntervalSec:   10,
		ConfigReloadIntervalSec: 20,
	}
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	if other.StateStoreIntervalSec != 0 {
		c.StateStoreIntervalSec = other.StateStoreIntervalSec
	}
	if other.ConfigReloadIntervalSec != 0 {
		c.ConfigReloadIntervalSec = other.ConfigReloadIntervalSec
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
	if c.ConfigReloadIntervalSec <= 0 {
		return fmt.Errorf("invalid ConfigReloadIntervalSec=%v, must be > 0sec", c.ConfigReloadIntervalSec)
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

func (c *Config) Reload() error {
	var (
		err error
		nc  *Config
	)
	if c.ReloadFn != nil {
		nc, err = c.ReloadFn()
		if err == nil {
			err = nc.Check()
			if err == nil {
				c.Apply(nc)
			}
		}
	}
	return err
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}

//===================== workerConfig =====================

func (wc *WorkerConfig) Check() error {
	if strings.TrimSpace(wc.Name) == "" {
		return fmt.Errorf("invalid Name=%v, must be non-empty", wc.Name)
	}
	if wc.Source == nil {
		return fmt.Errorf("invalid Source=%v, must be non-nil", wc.Source)
	}
	if wc.Sink == nil {
		return fmt.Errorf("invalid Sink=%v, must be non-nil", wc.Sink)
	}

	err := wc.Source.Check()
	if err != nil {
		return fmt.Errorf("invalid Source=%v: %v", wc.Source, err)
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

//===================== sourceConfig =====================

func (sc *SourceConfig) Check() error {
	if strings.TrimSpace(sc.Lql) == "" {
		return fmt.Errorf("invalid Lql=%v, must be non-empty", sc.Lql)
	}
	if _, err := lql.ParseLql(sc.Lql); err != nil {
		return fmt.Errorf("invalid Lql=%s: %v", sc.Lql, err)
	}
	if sc.Filter != "" {
		if _, err := syntax.Parse(sc.Filter, syntax.Perl); err != nil {
			return fmt.Errorf("invalid Filter=%s: %v", sc.Filter, err)
		}
	}
	return nil
}

func (sc *SourceConfig) String() string {
	return utils.ToJsonStr(sc)
}
