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

	// Ingestor provides Wrtie method for sending log data into the storage. This intrface is exposed as
	// a public API
	Ingestor interface {
		// Write sends log events into the stream
		Write(src, tags string, evs []*LogEvent) error
	}
)
