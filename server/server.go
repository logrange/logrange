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

package server

import (
	"context"

	"github.com/jrivets/log4g"
	"github.com/logrange/linker"
	"github.com/logrange/logrange/pkg/cluster/model"
	"github.com/logrange/logrange/pkg/kv/inmem"
)

// Start starts the logrange server using the configuration provided. It will
// stop it as soon as ctx is closed
func Start(ctx context.Context, cfg *Config) error {
	log := log4g.GetLogger("server")
	log.Info("Start with config:", cfg)

	injector := linker.New()
	injector.SetLogger(log4g.GetLogger("injector"))
	injector.Register(
		linker.Component{Name: "", Value: inmem.New()},
		linker.Component{Name: "HostRegistryConfig", Value: cfg},
		linker.Component{Name: "", Value: model.NewHostRegistry()},
		linker.Component{Name: "", Value: model.NewJournalCatalog()},
	)

	injector.Init(ctx)

	select {
	case <-ctx.Done():

	}
	injector.Shutdown()

	return nil
}
