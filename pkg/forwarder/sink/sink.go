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

package sink

import (
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/utils"
)

type (
	Params map[string]interface{}

	Config struct {
		Type   string
		Params Params
	}

	Sink interface {
		OnEvent(e []*api.LogEvent) error
		Close() error
	}
)

const (
	SnkTypeStdout = "stdout"
	SnkTypeSyslog = "syslog"
)

func NewSink(cfg *Config) (Sink, error) {
	switch cfg.Type {
	case SnkTypeStdout:
		return newStdSkink(&stdoutSinkConfig{})
	case SnkTypeSyslog:
		scfg, err := newSyslogSinkConfig(cfg.Params)
		if err == nil {
			return newSyslogSink(scfg)
		}
		return nil, err
	}

	return nil, fmt.Errorf("unknown Type=%v", cfg.Type)
}

//===================== config =====================

func (c *Config) Check() error {
	switch c.Type {
	case SnkTypeStdout:
		return nil
	case SnkTypeSyslog:
		cfg, err := newSyslogSinkConfig(c.Params)
		if err == nil {
			return cfg.Check()
		}
		return err
	}

	return fmt.Errorf("unknown Type=%v", c.Type)
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
