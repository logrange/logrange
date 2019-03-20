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

package syslog

import (
	"fmt"
	"github.com/logrange/logrange/pkg/utils"
)

type (
	Config struct {
		Protocol          string
		RemoteAddr        string
		RootCAFile        string
		ReplaceNewLine    *bool
		LineLenLimit      *int
		ConnectTimeoutSec *int
		WriteTimeoutSec   *int
	}
)

//===================== logger =====================

func NewDefaultConfig() *Config {
	return &Config{
		Protocol:          "tcp",
		RemoteAddr:        "127.0.0.1:514",
		ReplaceNewLine:    utils.BoolPtr(true),
		ConnectTimeoutSec: utils.IntPtr(10),
		WriteTimeoutSec:   utils.IntPtr(5),
		LineLenLimit:      utils.IntPtr(1024),
	}
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	if other.Protocol != "" {
		c.Protocol = other.Protocol
	}
	if other.RemoteAddr != "" {
		c.RemoteAddr = other.RemoteAddr
	}
	if other.RootCAFile != "" {
		c.RootCAFile = other.RootCAFile
	}
	if v, ok := utils.PtrBool(other.ReplaceNewLine); ok {
		c.ReplaceNewLine = &v
	}
	if v, ok := utils.PtrInt(other.LineLenLimit); ok {
		c.LineLenLimit = &v
	}
	if v, ok := utils.PtrInt(other.ConnectTimeoutSec); ok {
		c.ConnectTimeoutSec = &v
	}
	if v, ok := utils.PtrInt(other.WriteTimeoutSec); ok {
		c.WriteTimeoutSec = &v
	}
}

func (c *Config) Check() error {
	if c.Protocol != ProtoTLS && c.Protocol != ProtoUDP && c.Protocol != ProtoTCP {
		return fmt.Errorf("invalid config; unknown Protocol=%v", c.Protocol)
	}
	if c.Protocol != ProtoTLS && c.RootCAFile != "" {
		return fmt.Errorf("invalid config; RootCAFile=%v, must be empty if Protocol == %v",
			c.RootCAFile, c.Protocol)
	}
	if c.RemoteAddr == "" {
		return fmt.Errorf("invalid config; RemoteAddr=%v, must be non-empty", c.RemoteAddr)
	}
	if v, ok := utils.PtrInt(c.LineLenLimit); ok && v < 0 {
		return fmt.Errorf("invalid config; LineLenLimit=%v, must be >= 0", c.LineLenLimit)
	}
	if v, ok := utils.PtrInt(c.ConnectTimeoutSec); ok && v < 0 {
		return fmt.Errorf("invalid config; ConnectTimeoutSec=%v, must be >= 0", c.ConnectTimeoutSec)
	}
	if v, ok := utils.PtrInt(c.WriteTimeoutSec); ok && v < 0 {
		return fmt.Errorf("invalid config; WriteTimeoutSec=%v, must be >= 0", c.WriteTimeoutSec)
	}
	return nil
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
