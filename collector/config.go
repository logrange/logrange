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

package collector

import (
	"encoding/json"
	"fmt"
	"github.com/logrange/logrange/collector/remote"
	"github.com/logrange/logrange/collector/scanner"
	"github.com/logrange/logrange/collector/storage"
	"io/ioutil"
)

// Config struct just aggregate different types of configs in one place
type Config struct {
	Remote  *remote.Config  `json:"remote"`
	Scanner *scanner.Config `json:"scanner"`
	Storage *storage.Config `json:"storage"`
}

//===================== config =====================

func NewDefaultConfig() *Config {
	return &Config{
		Remote:  remote.NewDefaultConfig(),
		Scanner: scanner.NewDefaultConfig(),
		Storage: storage.NewDefaultConfig(),
	}
}

func LoadCfgFromFile(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	err = json.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}

	c.Remote.Apply(other.Remote)
	c.Scanner.Apply(other.Scanner)
	c.Storage.Apply(other.Storage)
}

func (c *Config) Check() error {
	if c.Remote == nil {
		return fmt.Errorf("invalid config; remote=%v, must be non-nil", c.Remote)
	}
	if c.Scanner == nil {
		return fmt.Errorf("invalid config; scanner=%v, must be non-nil", c.Scanner)
	}
	if c.Storage == nil {
		return fmt.Errorf("invalid config; storage=%v, must be non-nil", c.Storage)
	}
	if err := c.Remote.Check(); err != nil {
		return err
	}
	if err := c.Scanner.Check(); err != nil {
		return err
	}
	if err := c.Storage.Check(); err != nil {
		return err
	}
	return nil
}
