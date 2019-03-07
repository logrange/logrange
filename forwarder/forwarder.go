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

package forwarder

import (
	"context"
	"github.com/jrivets/log4g"
)

var (
	logger = log4g.GetLogger("forwarder")
)

// Run executes the forwarder process using ctx context provided and the forwarder configuration
func Run(ctx context.Context, cfg *Config) error {

	<-ctx.Done()

	logger.Info("Shutdown.")
	return nil
}
