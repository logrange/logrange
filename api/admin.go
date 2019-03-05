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

package api

import "context"

type (
	// SourceTruncateResult is returned as a result by applying truncate procedure to the source provided
	SourceTruncateResult struct {
		// Tags contains the source tags
		Tags string
		// SizeBefore contains the size of the source before truncation
		SizeBefore uint64
		// SizeAfter contains the size of the source after truncation
		SizeAfter uint64
		// RecordsBefore number of records before truncation
		RecordsBefore uint64
		// RecordsAfter number of records after truncation
		RecordsAfter uint64
	}

	// TruncateResult contains the result of the Truncate execution
	TruncateResult struct {
		// Sources contains all affected sources
		Sources []*SourceTruncateResult

		// Err contains the operation error, if any
		Err error `josn:"-"`
	}

	// Admin interface allows to perform some administrative actions
	Admin interface {

		// Truncate affects all sources whcih meet the tagsCond and have the size equal or bigger than maxSize
		// Returns list of sources affected.
		Truncate(ctx context.Context, tagsCond string, maxSize uint64) (res TruncateResult, err error)
	}
)
