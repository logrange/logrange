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
	"github.com/logrange/logrange/pkg/syslog"
	"github.com/logrange/logrange/pkg/utils"
	"time"
)

type (
	Config struct {
		Type   string
		Params map[string]interface{}
	}

	Sink interface {
		OnEvent(e *api.LogEvent) error
		Close() error
	}

	stdoutSink struct {
	}

	syslogSink struct {
		slog *syslog.Logger
	}
)

const (
	SnkTypeStdout = "stdout"
	SnkTypeSyslog = "syslog"
)

const (
	PrmSyslogProtocol   = "Protocol"
	PrmSyslogRemoteAddr = "RemoteAddr"
	PrmSyslogTlsCAFile  = "TlsCAFile"

	//add more params...
)

func NewSink(cfg *Config) (Sink, error) {
	if err := cfg.Check(); err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}

	switch cfg.Type {
	case SnkTypeStdout:
		return newStdSkink(cfg), nil
	case SnkTypeSyslog:
		return newSyslogSkink(cfg)
	}

	return nil, fmt.Errorf("unknown sink type=%v", cfg.Type)
}

//===================== stdoutSink =====================

func newStdSkink(cfg *Config) Sink {
	return &stdoutSink{}
}

func (ss *stdoutSink) OnEvent(e *api.LogEvent) error {
	fmt.Print(e.Message)
	return nil
}

func (ss *stdoutSink) Close() error {
	return nil
}

//===================== syslogSink =====================

func newSyslogSkink(cfg *Config) (Sink, error) {
	slog, err := syslog.NewLogger(&syslog.Config{
		Protocol:   cfg.Params[PrmSyslogProtocol].(string),
		RemoteAddr: cfg.Params[PrmSyslogRemoteAddr].(string),
		RootCAFile: cfg.Params[PrmSyslogTlsCAFile].(string),
		//add more params...
	})

	if err != nil {
		return nil, fmt.Errorf("failed creating syslog logger, err=%v", err)
	}

	return &syslogSink{
		slog: slog,
	}, nil
}

func (ss *syslogSink) OnEvent(e *api.LogEvent) error {
	m := &syslog.Message{
		Severity: syslog.SeverityInfo,
		Facility: syslog.FacilityLocal6,
		Time:     time.Unix(0, int64(e.Timestamp)),
		Hostname: "localhost",
		Tag:      e.Tags,
		Msg:      e.Message,
	}
	return ss.slog.Write(m)
}

func (ss *syslogSink) Close() error {
	if ss.slog != nil {
		return ss.slog.Close()
	}
	return nil
}

//===================== config =====================

func (c *Config) Check() error {
	if c.Type != SnkTypeStdout && c.Type != SnkTypeSyslog {
		return fmt.Errorf("unknown Type=%v", c.Type)
	}

	var pp []string

	switch c.Type {
	case SnkTypeStdout:
	case SnkTypeSyslog:
		pp = []string{PrmSyslogProtocol, PrmSyslogRemoteAddr}
	}

	for _, p := range pp {
		if err := c.checkParamExists(p); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) checkParamExists(pName string) error {
	if c.Params != nil {
		if _, ok := c.Params[pName]; ok {
			return nil
		}
	}
	return fmt.Errorf("invalid Params=%v, must have param '%v'", c.Params, pName)
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
