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
	"errors"
	"fmt"
	"github.com/logrange/logrange/pkg/scanner/parser"
	"github.com/logrange/logrange/pkg/utils"
	"regexp"
	"regexp/syntax"
	"strings"
)

type (
	// SchemaConfig describes the configuration for parsing of the data
	SchemaConfig struct {
		// PathMatcher contains a regexp for matching file names. It can contain
		// named groups, which values can be assigned for tags and fields (see Meta field)
		PathMatcher string
		DataFormat  parser.DataFormat
		DateFormats []string
		// Meta describes Tags and fields assignment. It is formded based on PathMather
		// where keys are taken by group names from the regular expression

		Meta Meta
	}

	// Meta descirbes Tags and Fields assignment. Tags and Fields are maps, where
	// key is the value of Tag or Field and the value is a template for the value. The
	// template can contains variables placed in curly braces e.g. "var1={var1} or var2={var2}"...
	Meta struct {
		// Tags contains map of key-value pairs
		Tags map[string]string

		// Fields contains map of key-value pairs
		Fields map[string]string
	}

	schema struct {
		cfg     *SchemaConfig
		matcher *regexp.Regexp
	}
)

//===================== schema =====================

func newSchema(cfg *SchemaConfig) *schema {
	return &schema{
		cfg:     cfg,
		matcher: regexp.MustCompile(cfg.PathMatcher),
	}
}

func (s *schema) getMeta(d *desc) Meta {
	vars := s.getVars(d.File)
	tags := make(map[string]string, len(s.cfg.Meta.Tags))
	for k, v := range s.cfg.Meta.Tags {
		tags[k] = s.subsVars(v, vars)
	}

	fields := make(map[string]string, len(s.cfg.Meta.Fields))
	for k, v := range s.cfg.Meta.Fields {
		fields[k] = s.subsVars(v, vars)
	}

	return Meta{
		Tags:   tags,
		Fields: fields,
	}
}

func (s *schema) getVars(l string) map[string]string {
	names := s.matcher.SubexpNames()
	match := s.matcher.FindStringSubmatch(l)

	if len(names) > 1 {
		names = names[1:] //skip ""
	}
	if len(match) > 1 {
		match = match[1:] //skip "" value
	}

	vars := make(map[string]string, len(names))
	for i, n := range names {
		if len(match) > i {
			vars[n] = match[i]
		} else {
			vars[n] = ""
		}
	}
	return vars
}


func (s *schema) subsVars(l string, vars map[string]string) string {
	for k, v := range vars {
		l = strings.Replace(l, "{"+k+"}", v, -1)
	}
	return l
}

//===================== schemaConfig =====================

func (sc *SchemaConfig) Check() error {
	if strings.TrimSpace(sc.PathMatcher) == "" {
		return errors.New("PatchMatcher must be non-empty")
	}
	_, err := syntax.Parse(sc.PathMatcher, syntax.Perl)
	if err != nil {
		return fmt.Errorf("PathMatcher=%v is invalid; %v", sc.PathMatcher, err)
	}
	if sc.DataFormat != parser.FmtK8Json && sc.DataFormat != parser.FmtText && sc.DataFormat != parser.FmtPure && sc.DataFormat != parser.FmtLogfmt{
		return fmt.Errorf("DataFormat is unknown=%v", sc.DataFormat)
	}
	return nil
}

func (sc *SchemaConfig) String() string {
	return utils.ToJsonStr(sc)
}
