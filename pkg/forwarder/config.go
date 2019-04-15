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
	// PipeConfig struct constains settings for filtering records from different partitions
	PipeConfig struct {
		// From contains an expression for selecting partitions where records will be considered
		// The value could be empty - app partitions
		From string
		// Filter contains an expression for filtering records (true means record is taken).
		// The value could be empty - all records match
		Filter string
	}

	// WorkerConfig struct sets up the name of forwarder, the source (Pipe) and the records
	// destination (Sink)
	WorkerConfig struct {
		// Name contains the name of the forwarding configuration
		Name string
		// Pipe describes the source, where records will be taken
		Pipe *PipeConfig
		// Sink describes the destination, where records will be written
		Sink *sink.Config
	}

	// Config struct contains the comprehensive forwarder configuration. It describes
	// workers, and some common parameters
	Config struct {
		// Workers slice contains configuration for all workers
		Workers []*WorkerConfig
		// StateStoreIntervalSec the number of seconds between saving state calls
		StateStoreIntervalSec int
		// SyncWorkersIntervalSec the number of second between re-checking configurations (files)
		SyncWorkersIntervalSec int
		// ReloadFn the function which is called for re-load the config (Read from a file, for instance)
		ReloadFn func() (*Config, error) `json:"-"`
	}
)

//===================== config =====================

// NewDefaultConfig creates a new instance of Config with default values
func NewDefaultConfig() *Config {
	return &Config{
		Workers:                []*WorkerConfig{},
		StateStoreIntervalSec:  10,
		SyncWorkersIntervalSec: 20,
	}
}

// Apply allows to overwrite existing values by the other config provided
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

// Check performs a parameter checks and returns an error if they are not acceptable
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

// Reload refresh and can update the Config c instance values
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

// Equals returns true if the Config c has same field values as other
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

// Check performs an internal check for WorkerConfig fields
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

// String is fmt.Stringer implementation
func (wc *WorkerConfig) String() string {
	return utils.ToJsonStr(wc)
}

//===================== streamConfig =====================

// Check performs an internal check for PipeConfig fields
func (sc *PipeConfig) Check() error {
	if _, err := lql.ParseSource(sc.From); err != nil {
		return fmt.Errorf("invalid From=%s: %v", sc.From, err)
	}
	if _, err := lql.ParseExpr(sc.Filter); err != nil {
		return fmt.Errorf("invalid Filter=%s: %v", sc.Filter, err)
	}
	if sc.Filter != "" {
		if _, err := syntax.Parse(sc.Filter, syntax.Perl); err != nil {
			return fmt.Errorf("invalid Filter=%s: %v", sc.Filter, err)
		}
	}
	return nil
}

// String is fmt.Stringer implementation
func (sc *PipeConfig) String() string {
	return utils.ToJsonStr(sc)
}
