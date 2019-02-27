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

package remote

import (
	"fmt"
	"github.com/logrange/logrange/collector/utils"
	"github.com/logrange/range/pkg/transport"
)

type (
	Config struct {
		Transport               *transport.Config
		ConnectMaxRetry         int
		ConnectRetryIntervalSec int
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	cfg := new(Config)
	cfg.Transport = &transport.Config{
		ListenAddr: "127.0.0.1:9966",
	}
	cfg.ConnectMaxRetry = 3
	cfg.ConnectRetryIntervalSec = 5
	return cfg
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	c.Transport.Apply(other.Transport)
	if other.ConnectRetryIntervalSec > 0 {
		c.ConnectRetryIntervalSec = other.ConnectRetryIntervalSec
	}
	if other.ConnectMaxRetry > 0 {
		c.ConnectMaxRetry = other.ConnectMaxRetry
	}
}

func (c *Config) Check() error {
	if err := c.Transport.Check(); err != nil {
		return err
	}
	if c.ConnectRetryIntervalSec < 1 {
		return fmt.Errorf("invalid config; "+
			"ConnectRetryIntervalSec=%d, must be >= 1sec", c.ConnectRetryIntervalSec)
	}
	if c.ConnectMaxRetry < 0 {
		return fmt.Errorf("invalid config; "+
			"ConnectMaxRetry=%d, must be >= 0", c.ConnectMaxRetry)
	}
	return nil
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
