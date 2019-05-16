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
	"github.com/logrange/logrange/pkg/partition"
	"github.com/logrange/range/pkg/records/journal"
)

// ItFactory interface is used for creating new cursors.
type ItFactory interface {
	// GetJournals returns map of journals by tags:partition.Journal or an error, if any. The returned journals
	// must be released after usage by Release() function
	GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]journal.Journal, error)

	// Iterator constructs new iterator for partition src. If the tmRange is provided the
	// iterator can utilize time-index for finding records
	Itearator(j journal.Journal, tmRange *model.TimeRange) journal.Iterator

	// Release releases the partition. Journal must not be used after the call
	Release(jn string)
}

type itfactory struct {
	Parts *partition.Service `inject:""`
}

// NewItFactory provides ItFactory implementation which based on partition.Service
func NewItFactory() ItFactory {
	return new(itfactory)
}

// GetJournals is part of ItFactory
func (itf *itfactory) GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]journal.Journal, error) {
	return itf.Parts.GetJournals(ctx, tagsCond, maxLimit)
}

// Iterator is part of ItFactory
func (itf *itfactory) Itearator(j journal.Journal, tmRange *model.TimeRange) journal.Iterator {
	if tmRange == nil {
		return journal.NewJIterator(j)
	}

	return partition.NewJIterator(*tmRange, j, itf.Parts.TsIndexer, itf.Parts.GetTmIndexRebuilder())
}

// Release is part of ItFactory
func (itf *itfactory) Release(jn string) {
	itf.Parts.Release(jn)
}
