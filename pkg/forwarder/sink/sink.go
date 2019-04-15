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
	// Params a map of key-value pairs which allows to provide some settings to
	// a Sink implementation
	Params map[string]interface{}

	// Config struct contains config for an abstract Sink.
	Config struct {
		// Type defines type of Sink
		Type string
		// Params contains params for the specified type
		Params Params
	}

	// Sink interface is an abstraction for a sink implementation
	Sink interface {
		// OnEvent is called when new portion of events should be sent to the sink
		OnEvent(events []*api.LogEvent) error
		// Close is part of io.Closer
		Close() error
	}
)

const (
	SnkTypeStdout = "stdout"
	SnkTypeSyslog = "syslog"
)

// NewSink creates a new Sink instance by cfg provided. "stdout" and "syslog" is only supported so far
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

// Check peforms an internal check of c fields
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

// String is fmt.Stringer implementation
func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
