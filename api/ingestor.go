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

	// WriteResult struct contains result of Ingestor.Write operation execution.
	WriteResult struct {
		Err error
	}

	// Ingestor provides Wrtie method for sending log data into the storage. This intrface is exposed as
	// a public API
	Ingestor interface {
		// Write sends log events into the stream identified by tag provided. It expects a slice of events and
		// the reference to the WriteResult. Tags and Fields fields in LogEvents are ignored during the writing operation,
		// but tags and fields params will be applied to all of the events.
		Write(ctx context.Context, tags, fields string, evs []*LogEvent, res *WriteResult) error
	}
)
