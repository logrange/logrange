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

package storage

import (
	"fmt"
	"github.com/logrange/logrange/collector/utils"
	"strings"
)

type (
	Config struct {
		Type     StorageType `json:"type"`
		Location string      `json:"location"`
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	return &Config{
		Type: TypeInMem,
	}
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	if other.Type != "" {
		c.Type = other.Type
	}
	if strings.TrimSpace(other.Location) != "" {
		c.Location = other.Location
	}
}

func (c *Config) Check() error {
	if c.Type == "" {
		return fmt.Errorf("invalid config; type=%v, must be non-empty", c.Type)
	}

	switch c.Type {
	case TypeFile:
		if strings.TrimSpace(c.Location) == "" {
			return fmt.Errorf("invalid config; location=%v, "+
				"must be non-empty for type=%v", c.Location, c.Type)
		}
	case TypeInMem:
		if strings.TrimSpace(c.Location) != "" {
			return fmt.Errorf("invalid config; location=%v, "+
				"must be empty for type=%v", c.Location, c.Type)
		}
	default:
		return fmt.Errorf("invalid config; unknown type=%v", c.Type)
	}

	return nil
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
