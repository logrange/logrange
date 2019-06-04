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
	// Pipes allows to manage streams
	Pipes interface {
		// Create a new pipe if it doesn't exist. The function will return an error,
		// if the function could not connet or reach the server by any reason. If the communiction
		// was ok, but the operation was failed on the server side by any reason, the function
		// will return nil, but res.Err will contain the server error.
		EnsurePipe(ctx context.Context, p Pipe, res *PipeCreateResult) error
	}

	// Pipe struct describes a pipe
	Pipe struct {
		// Name contains the pipe name, which must be unique
		Name string

		// TagsCond contains the condition, which filters sources for the pipe
		TagsCond string

		// FilterCond desribes the filtering condition. Only events, for which the condition
		// is true, will be piped
		FilterCond string

		// Destination contains tags conditions used for the pipe destination. This field is
		// defined by server, so it is ignored by Pipes.EnsurePipe
		Destination string
	}

	// PipeCreateResult struct describes the result of Pipes.EnsurePipe function
	PipeCreateResult struct {
		// Pipe contains created pipe object
		Pipe Pipe

		// Err the operaion error, if any.
		Err error `json:"-"`
	}
)
