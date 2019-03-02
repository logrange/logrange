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

package cli

import (
	"github.com/logrange/range/pkg/transport"
)

type (
	Config struct {
		Transport  *transport.Config
		StreamMode bool
		Query      []string
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	cfg := new(Config)
	cfg.Transport = &transport.Config{}
	cfg.Transport.ListenAddr = "127.0.0.1:9966"
	return cfg
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	c.Transport.Apply(other.Transport)
}

func (c *Config) Check() error {
	if err := c.Transport.Check(); err != nil {
		return err
	}
	return nil
}
