// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a MakeCopy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"fmt"
	"strings"
)

type (
	Config struct {
		File            string
		MaxRecSizeBytes int
		DataFmt         DataFormat
		DateFmts        []string
		FieldMap        map[string]string
	}
)

func (c *Config) Check() error {
	if strings.TrimSpace(c.File) == "" {
		return fmt.Errorf("invalid File=%v, must be non-empty", c.File)
	}

	switch c.DataFmt {
	case FmtText:
	case FmtPure:
		return c.errorIfDateFormats()
	case FmtK8Json:
		return c.errorIfDateFormats()
	case FmtLogfmt:
		return c.errorIfDateFormats()
	default:
		return fmt.Errorf("unknown DataFmt=%v", c.DataFmt)
	}

	return nil
}

func (c *Config) errorIfDateFormats() error {
	if len(c.DateFmts) > 0 {
		return fmt.Errorf("invalid DateFmts=%v, "+
			"must be empty for DataFmt=%v", c.DateFmts, c.DataFmt)
	}
	return nil
}
