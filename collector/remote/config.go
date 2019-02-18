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

package remote

import (
	"fmt"
	"github.com/logrange/logrange/collector/utils"
	"strings"
)

type (
	Config struct {
		Server   string `json:"server"`
		RetrySec int    `json:"retrySec"`
		MaxRetry int 	`json:"maxRetry"`
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	cfg := new(Config)
	cfg.Server = "127.0.0.1:9966"
	cfg.RetrySec = 5
	cfg.MaxRetry = 3
	return cfg
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}

	if strings.TrimSpace(other.Server) != "" {
		c.Server = other.Server
	}
	if other.RetrySec > 0 {
		c.RetrySec = other.RetrySec
	}
	if other.MaxRetry > 0 {
		c.MaxRetry = other.MaxRetry
	}
}

func (c *Config) Check() error {
	if strings.TrimSpace(c.Server) == "" {
		return fmt.Errorf("invalid config; server=%v, must be non-empty", c.Server)
	}
	if c.RetrySec < 1 {
		return fmt.Errorf("invalid config; retrySec=%d, must be >= 1sec", c.RetrySec)
	}
	if c.MaxRetry < 0 {
		return fmt.Errorf("invalid config; maxRetry=%d, must be >= 0", c.RetrySec)
	}
	return nil
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
