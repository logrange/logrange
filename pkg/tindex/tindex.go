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

		// GetJournals returns map of matched Tags to the journals they address by the source expression
		// the function receive maxSize of the result map and the flag checkAll which allows to count total matches found.
		// It returns the resulted map, number or matches in total (if checkAll is provided) and an error if any
		GetJournals(srcCond *lql.Source, maxSize int, checkAll bool) (map[tag.Line]string, int, error)
	}
)
