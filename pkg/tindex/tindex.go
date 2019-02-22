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

package tindex

import (
	"github.com/logrange/logrange/pkg/lql"
)

type (
	// Service interface provides an access to the tags index. It is used for selecting
	// a journal sources by the tag lines provided and by selecting journals by an expression.
	Service interface {
		// GetOrCreateJournal returns the journal name for the unique tags combination
		GetOrCreateJournal(tags string) (string, error)

		// GetJournals returns list of journals by the compiled tags source expression
		GetJournals(exp *lql.Expression) ([]string, error)
	}
)
