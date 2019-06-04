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

	// Ingestor provides Wrtie method for sending records into a partition.
	Ingestor interface {
		// Write sends events into the partition identified by tags provided. It expects a slice of events and
		// LogEvent(s) and a pointer to the WriteResult. 'Tags' and 'Fields' fields in LogEvents
		// are ignored during the writing operation, but tags and fields params will be applied to all of the events.
		//
		// The function returns error if any communication problem to the server happens.
		// If the Write operation could reach the server, but the operation was failed there by any
		// reason, the res variable will contain the error result from server (res.Err)
		Write(ctx context.Context, tags, fields string, evs []*LogEvent, res *WriteResult) error
	}

	// WriteResult struct contains result of Ingestor.Write operation execution.
	WriteResult struct {
		// Err contains the error which happens on the server side. So the Ingestor.Write
		// call could be successful (no error is returned), but the Err field in res parameter
		// (see Ingestor.Write) wil be not nil. The error from the server is here.
		Err error
	}
)
