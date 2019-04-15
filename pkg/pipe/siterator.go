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

package pipe

import (
	"context"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/utils/bytes"
)

type (
	// siterator struct provides model.Iterator functionality by adding extra fields extFlds to every record fields list
	siterator struct {
		// extFlds contains extra fields that will be added to every record's field list
		extFlds field.Fields
		it      model.Iterator
		w       bytes.Writer
		rfld    field.Fields
		ready   bool
	}
)

func (si *siterator) init(extFlds field.Fields, it model.Iterator) {
	si.extFlds = extFlds
	si.it = it
}

func (si *siterator) Next(ctx context.Context) {
	si.it.Next(ctx)
	si.ready = false
}

// Get returns current LogEvent, the TagsCond for the event or an error if any. It returns io.EOF when end of the collection is reached
func (si *siterator) Get(ctx context.Context) (model.LogEvent, tag.Line, error) {
	le, ln, err := si.it.Get(ctx)
	if err != nil {
		return le, ln, err
	}

	if !si.ready {
		si.w.Reset()
		si.rfld = le.Fields.Concat(si.extFlds, &si.w)
		si.ready = true
	}

	le.Fields = si.rfld
	return le, ln, nil
}

func (si *siterator) Release() {
	si.it.Release()
	si.ready = false
}
