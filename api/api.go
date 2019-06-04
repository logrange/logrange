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

// Package api contains structures and data-type definitions that could be used
// for accessing the Logrange database using the api.Client interface.
//
// This is version v0 of the api and the following rules must be obeyed when
// a change is needed:
//  - Names of existing data-types cannot be changed
//  - Names of struct fields must be capitalized and cannot be changed
//  - Types of already existing fields cannot be changed
//  - Functions params and signatures cannot be changed.
//  - New fields could be added to the existing data structures
//  - New types and structures could be added
//  - New functions could be added either to existing interfaces or to the package
package api

type (
	// LogEvent struct describes a partition record.
	LogEvent struct {
		// Timestamp contains the time-stamp for the message. It contains a timestamp,
		// associated with the event, in nano-seconds
		Timestamp int64

		// Message contains the event data
		Message string

		// Tag line associated with the event. It could be empty when the new records ingested
		// (write operation). The tag line has the form like `tag1=value1,tag2=value2...`
		Tags string

		// Fields associated with the event. The field could be empty.
		// The fields line has the form like `field1=value1,field2=value2...`
		Fields string
	}
)
