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
		// ChunksDeleted contains number of chunks deleted
		ChunksDeleted int
		// Deleted contains whether the source was completely deleted
		Deleted bool
	}

	// TruncateResult contains the result of the Truncate execution
	TruncateResult struct {
		// Sources contains all affected sources
		Sources []*SourceTruncateResult

		// Err contains the operation error, if any
		Err error `json:"-"`
	}

	// TruncateRequest contains truncation params
	TruncateRequest struct {
		// DryRun will not affect data, but wil report sources affected
		DryRun bool
		// TagsCond contains the tags condition to select journals to be truncated
		TagsCond string
		// MaxSrcSize defines the upper level of a journal size, which will be truncated, if reached
		MaxSrcSize uint64
		// MinSrcSize defines the lower level of a journal size, which will not be cut if the journal will be less
		// than this parameter after truncation
		MinSrcSize uint64
		// OldestTs defines the oldest record timestamp. Chunks with records less than the parameter are candidates
		// for truncation
		OldestTs uint64
	}

	// Admin interface allows to perform some administrative actions
	Admin interface {

		// Truncate walks over all known sources and tries to truncate them if they match with the request
		// It returns list of sources affected.
		Truncate(ctx context.Context, req TruncateRequest) (res TruncateResult, err error)
	}
)
