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
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/forwarder"
	"github.com/logrange/logrange/pkg/storage"
)

func Run(ctx context.Context, cfg *forwarder.Config, cl api.Client, storg storage.Storage) error {

	logger := log4g.GetLogger("forwarder")
	fwd, err := forwarder.NewForwarder(cfg, cl, storg)
	if err != nil {
		return fmt.Errorf("failed to create forwarder, err=%v", err)
	}

	if err := fwd.Run(ctx); err != nil {
		return fmt.Errorf("failed to run forwarder, err=%v", err)
	}

	<-ctx.Done()
	_ = fwd.Close()

	logger.Info("Shutdown.")
	return err
}
