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

package main

import (
	"context"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/server"
	"github.com/logrange/range/pkg/cluster"
	"gopkg.in/urfave/cli.v2"
)

const (
	Version = "0.1.0"
)

const (
	argStartLogCfgFile = "log-config-file"
	argStartCfgFile    = "config-file"
	argStartHostHostId = "host-id"
	argStartJournalDir = "journals-dir"
	argStartNewTIdxOk  = "new-tindex-ok"
)

var cfg = server.GetDefaultConfig()

func main() {
	defer log4g.Shutdown()

	app := &cli.App{
		Name:    "logrange",
		Version: Version,
		Usage:   "Log Aggregation Service",
		Commands: []*cli.Command{
			{
				Name:   "start",
				Usage:  "Run logrange service",
				Action: runServer,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  argStartLogCfgFile,
						Usage: "The log4g configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartCfgFile,
						Usage: "The logrange configuration file path",
					},
					&cli.IntFlag{
						Name:  argStartHostHostId,
						Usage: "Unique host identifier, if 0 the id will be automatically assigned.",
						Value: int(cfg.HostHostId),
					},
					&cli.StringFlag{
						Name:  argStartJournalDir,
						Usage: "Defines path to the journals database directory",
						Value: cfg.JrnlCtrlConfig.JournalsDir,
					},
					&cli.BoolFlag{
						Name:  argStartNewTIdxOk,
						Usage: "Create a new tag-index if there is none",
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.FlagsByName(app.Commands[0].Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	if err := app.Run(os.Args); err != nil {
		getLogger().Fatal("Failed to run server, cause: ", err)
	}
}

func runServer(c *cli.Context) error {
	logCfgFile := c.String(argStartLogCfgFile)
	if logCfgFile != "" {
		err := log4g.ConfigF(logCfgFile)
		if err != nil {
			return err
		}
	}

	logger := getLogger()
	cfgFile := c.String(argStartCfgFile)
	if cfgFile != "" {
		logger.Info("Loading server config from=", cfgFile)
		config, err := server.ReadConfigFromFile(cfgFile)
		if err != nil {
			return err
		}
		cfg.Apply(config)
	}

	applyArgsToCfg(c, cfg)
	return server.Start(ctxWithSignalHandler(), cfg)
}

func applyArgsToCfg(c *cli.Context, cfg *server.Config) {
	dc := server.GetDefaultConfig()
	if hid := c.Int(argStartHostHostId); int(dc.HostHostId) != hid {
		cfg.HostHostId = cluster.HostId(hid)
	}
	if jd := c.String(argStartJournalDir); dc.JrnlCtrlConfig.JournalsDir != jd {
		cfg.JrnlCtrlConfig.JournalsDir = jd
	}
	if newIdx := c.Bool(argStartNewTIdxOk); newIdx {
		cfg.NewTIndexOk = true
	}
}

func ctxWithSignalHandler() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-sigChan:
			getLogger().Warn("Handling signal=", s)
			cancel()
		}
	}()
	return ctx
}

func getLogger() log4g.Logger {
	return log4g.GetLogger("logrange")
}
