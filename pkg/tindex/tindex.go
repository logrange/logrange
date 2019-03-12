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

package tindex

import (
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
)

type (
	// Service interface provides an access to the Tags index. It is used for selecting
	// a journal sources by the tag lines provided and by selecting journals by an expression.
	Service interface {
		// GetOrCreateJournal returns the journal name for the unique Tags combination
		GetOrCreateJournal(tags string) (string, error)

		// Visit walks over the tags-sources that corresponds to the srcCond.
		Visit(srcCond *lql.Source, v VisitorF) error
	}

	// VisitorF is the callback function which si called by Service.Visit for all matches found. It wil iterate
	// over the visit set until it is over or the function returns false
	VisitorF func(tags tag.Set, jrnl string) bool
)
