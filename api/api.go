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

package api

type (
	// LogEvent struct describes one message
	LogEvent struct {
		// Timestamp contains the time-stamp for the message.
		Timestamp int64
		// Message is the message itself
		Message string
		// Tag line for the message. It could be empty
		Tags string
	}

	// WriteResult struct contains result of Ingestor.Write operation execution.
	WriteResult struct {
		Err error
	}

	// Ingestor provides Wrtie method for sending log data into the storage. This intrface is exposed as
	// a public API
	Ingestor interface {
		// Write sends log events into the stream src. It expects stream name (src), tags, associated with the write,
		// a slice of events and the reference to the WriteResult. Tags field in LogEvents is ignored during the writing operation,
		// but tags param will be applied to all of the events.
		Write(src, tags string, evs []*LogEvent, res *WriteResult) error
	}

	// QeryResult is a result returned by the server in a response on LQL execution (see Querier.Query)
	QueryResult struct {
		Records []string
		Err     error
	}

	// Querier - executes a query agains logrange deatabase
	Querier interface {
		// Query runs lql to collect the server data and return it in the QueryResult. It returns an error which indicates
		// that the query could not be delivered to the server, or it did not happen.
		Query(lql string, res *QueryResult) error
	}
)
