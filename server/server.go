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

package server

import (
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/linker"
	"github.com/logrange/logrange/api/rpc"
	"github.com/logrange/logrange/pkg/backend"
	"github.com/logrange/logrange/pkg/cursor"
	"github.com/logrange/logrange/pkg/partition"
	"github.com/logrange/logrange/pkg/pipe"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/cluster/model"
	"github.com/logrange/range/pkg/kv/inmem"
	"github.com/logrange/range/pkg/records/journal/ctrlr"
	"github.com/logrange/range/pkg/utils/bytes"
	"path"
)

// Start starts the logrange server using the configuration provided. It will
// stop it as soon as ctx is closed
func Start(ctx context.Context, cfg *Config) error {
	// Adjusting known dirs
	tindexDir := path.Join(cfg.BaseDir, "tindex")
	cindexDir := path.Join(cfg.BaseDir, "cindex")
	pipeDir := path.Join(cfg.BaseDir, "pipes")
	dbDir := path.Join(cfg.BaseDir, "db")

	imsCfg := &tindex.InMemConfig{WorkingDir: tindexDir}
	cfg.PipesConfig.Dir = pipeDir
	cfg.JrnlCtrlConfig.JournalsDir = dbDir

	log := log4g.GetLogger("server")
	log.Info("Start with config:", cfg)

	injector := linker.New()
	injector.SetLogger(log4g.GetLogger("injector"))
	injector.Register(
		linker.Component{Name: "HostRegistryConfig", Value: cfg},
		linker.Component{Name: "JournalControllerConfig", Value: &cfg.JrnlCtrlConfig},
		linker.Component{Name: "cindexDir", Value: cindexDir},
		linker.Component{Name: "", Value: &cfg.PipesConfig},
		linker.Component{Name: "publicRpcTransport", Value: cfg.PublicApiRpc},
		linker.Component{Name: "tindexInMemCfg", Value: imsCfg},
		linker.Component{Name: "mainCtx", Value: ctx},
		linker.Component{Name: "", Value: new(bytes.Pool)},
		linker.Component{Name: "", Value: inmem.New()},
		linker.Component{Name: "", Value: tindex.NewInmemService()},
		linker.Component{Name: "", Value: partition.NewService()},
		linker.Component{Name: "", Value: pipe.NewService()},
		linker.Component{Name: "", Value: model.NewHostRegistry()},
		linker.Component{Name: "", Value: model.NewJournalCatalog()},
		linker.Component{Name: "", Value: rpc.NewServerIngestor()},
		linker.Component{Name: "", Value: rpc.NewServerQuerier()},
		linker.Component{Name: "", Value: rpc.NewServerAdmin()},
		linker.Component{Name: "", Value: rpc.NewServerPipes()},
		linker.Component{Name: "", Value: rpc.NewServer()},
		linker.Component{Name: "", Value: ctrlr.NewJournalController()},
		linker.Component{Name: "", Value: cursor.NewProvider()},
		linker.Component{Name: "", Value: backend.NewAdmin()},
		linker.Component{Name: "", Value: backend.NewQuerier()},
	)
	injector.Init(ctx)

	select {
	case <-ctx.Done():

	}
	injector.Shutdown()

	return nil
}
