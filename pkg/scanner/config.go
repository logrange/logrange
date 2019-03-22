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

package scanner

import (
	"fmt"
	"github.com/logrange/logrange/pkg/scanner/parser"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/mohae/deepcopy"
	"regexp/syntax"
)

type (
	Config struct {
		IncludePaths          []string
		ExcludeMatchers       []string
		ScanPathsIntervalSec  int
		StateStoreIntervalSec int
		RecordMaxSizeBytes    int
		EventMaxRecords       int
		Schemas               []*SchemaConfig
	}
)

//===================== config =====================

func NewDefaultConfig() *Config {
	return &Config{
		IncludePaths:          []string{"/var/log/*.log", "/var/log/*/*.log"},
		ScanPathsIntervalSec:  5,
		StateStoreIntervalSec: 5,
		RecordMaxSizeBytes:    16384,
		EventMaxRecords:       1000,
		Schemas: []*SchemaConfig{
			{
				PathMatcher: "/*(?:.+/)*(?P<file>.+\\..+)",
				DataFormat:  parser.FmtText,
				Meta: Meta{
					Tags: map[string]string{
						"file": "{file}",
					},
				},
			},
		},
	}
}

func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	if len(other.IncludePaths) != 0 {
		c.IncludePaths = deepcopy.Copy(other.IncludePaths).([]string)
	}
	if other.ScanPathsIntervalSec != 0 {
		c.ScanPathsIntervalSec = other.ScanPathsIntervalSec
	}
	if other.StateStoreIntervalSec != 0 {
		c.StateStoreIntervalSec = other.StateStoreIntervalSec
	}
	if len(other.Schemas) != 0 {
		c.Schemas = deepcopy.Copy(other.Schemas).([]*SchemaConfig)
	}
	if other.RecordMaxSizeBytes != 0 {
		c.RecordMaxSizeBytes = other.RecordMaxSizeBytes
	}
	if other.EventMaxRecords != 0 {
		c.EventMaxRecords = other.EventMaxRecords
	}
}

func (c *Config) Check() error {
	if len(c.IncludePaths) == 0 {
		return fmt.Errorf("invalid IncludePaths=%v, must be non-empty", c.IncludePaths)
	}
	if c.EventMaxRecords <= 0 {
		return fmt.Errorf("invalid EventMaxRecords=%v, must be > 0", c.EventMaxRecords)
	}
	if c.ScanPathsIntervalSec <= 0 {
		return fmt.Errorf("invalid ScanPathsIntervalSec=%v, must be > 0sec", c.ScanPathsIntervalSec)
	}
	if c.StateStoreIntervalSec <= 0 {
		return fmt.Errorf("invalid StateStoreIntervalSec=%v, must be > 0sec", c.StateStoreIntervalSec)
	}
	if c.RecordMaxSizeBytes < 64 || c.RecordMaxSizeBytes > 65536 {
		return fmt.Errorf("invalid RecordSizeMaxBytes=%v, must be in range [%v..%v]",
			c.RecordMaxSizeBytes, 64, 65536)
	}
	if len(c.Schemas) == 0 {
		return fmt.Errorf("invalid Schemas=%v, must be non-empty", c.Schemas)
	}
	for _, s := range c.Schemas {
		if err := s.Check(); err != nil {
			return fmt.Errorf("invalid Schema=%v: %v", s, err)
		}
	}
	for _, ex := range c.ExcludeMatchers {
		if _, err := syntax.Parse(ex, syntax.Perl); err != nil {
			return fmt.Errorf("invalid ExcludeMatchers=%s: %v", ex, err)
		}
	}
	return nil
}

func (c *Config) String() string {
	return utils.ToJsonStr(c)
}
