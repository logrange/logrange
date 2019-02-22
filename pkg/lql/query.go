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

package lql

import (
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/pkg/errors"
)

type (
	Query struct {
		sel  *Select
		wef  WhereExpFunc
		srcs []string
	}
)

// Compile takes an lql and tindex.Service, returning Query instance of an error if any
func Compile(lql string, tidx tindex.Service) (*Query, error) {
	sel, err := Parse(lql)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not parse request \"%s\"", lql)
	}

	wef, err := buildWhereExpFunc(sel.Where)
	if err != nil {
		return nil, errors.Wrapf(err, "Error in Where expression of %s", lql)
	}

	srcs, err := tidx.GetJournals(sel.Source)
	if err != nil {
		return nil, errors.Wrapf(err, "Error in Source expression of %s", lql)
	}

	q := new(Query)
	q.sel = sel
	q.srcs = srcs
	q.wef = wef

	return q, nil
}

// Filter returns true, if the provided LogEvent must be filtered (disregarded)
func (q *Query) Filter(le *model.LogEvent) bool {
	return !q.wef(le)
}
