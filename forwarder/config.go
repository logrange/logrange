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
	"encoding/json"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/range/pkg/transport"
	"io/ioutil"
)

type (
	ScanConfig struct {
		Type   string
		Lql    string
		Params map[string]interface{}
	}

	SinkConfig struct {
		Type           string
		FormatTemplate string
		Params         map[string]interface{}
	}

	// Config struct contains the forwarder configuration
	Config struct {
		// SinkConfig defines a config of sink. Default is empty will be selected by
		SinkConfig SinkConfig
		ScanConfig ScanConfig
		// Storage is the place where the forwarder state could be stored
		Storage   *storage.Config
		Transport transport.Config
	}
)

func NewDefaultConfig() *Config {
	cfg := &Config{Storage: storage.NewDefaultConfig()}
	cfg.Transport.ListenAddr = "127.0.0.1:9966"
	cfg.ScanConfig.Lql = "select source file=hfs_convert.log limit 10000"
	return cfg
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

	c.Storage.Apply(other.Storage)
}
