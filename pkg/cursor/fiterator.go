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

package cursor

import (
	"context"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
)

type (
	// fiterator struct is a wrapper for filtering events from an iterator
	fiterator struct {
		it    model.Iterator
		fltF  lql.WhereExpFunc
		le    model.LogEvent
		ln    tag.Line
		valid bool
	}
)

func newFIterator(it model.Iterator, wExp *lql.Expression) (*fiterator, error) {
	fltF, err := lql.BuildWhereExpFuncByExpression(wExp)
	if err != nil {
		return nil, err
	}

	fit := new(fiterator)
	fit.it = it
	fit.fltF = fltF
	return fit, nil
}

// Next switches to the next event, if any
func (fit *fiterator) Next(ctx context.Context) {
	fit.it.Next(ctx)
	fit.le.Release()
	fit.valid = false
}

// Get returns current LogEvent, the TagsCond for the event or an error if any. It returns io.EOF when end of the collection is reached
func (fit *fiterator) Get(ctx context.Context) (model.LogEvent, tag.Line, error) {
	var err error
	for !fit.valid {
		fit.le, fit.ln, err = fit.it.Get(ctx)
		if err != nil {
			break
		}

		fit.valid = fit.fltF(&fit.le)
		if !fit.valid {
			fit.Next(ctx)
		}
	}
	return fit.le, fit.ln, err
}

func (fit *fiterator) Release() {
	fit.it.Release()
	if fit.valid {
		fit.le.MakeItSafe()
	}
}
