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

package main

import (
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/logrange/server"
	"github.com/logrange/range/pkg/cluster"
	"gopkg.in/urfave/cli.v2"
	"os"
	"sort"
)

const (
	argStartLogCfgFile = "log-config-file"
	argStartCfgFile    = "config-file"
	argStartHostHostId = "host-id"
	argStartJournalDir = "journals-dir"
	argMaxReadFDS      = "max-read-fds"
)

var cfg = server.GetDefaultConfig()

func main() {
	defer log4g.Shutdown()

	app := &cli.App{
		Name:    "logrange",
		Version: logrange.Version,
		Usage:   "Log Aggregation Service",
		Commands: []*cli.Command{
			{
				Name:   "start",
				Usage:  "Run logrange service",
				Action: runServer,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  argStartLogCfgFile,
						Usage: "log4g configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartCfgFile,
						Usage: "server configuration file path",
					},
					&cli.IntFlag{
						Name:  argStartHostHostId,
						Usage: "unique host id, if 0 the id will be automatically assigned",
					},
					&cli.StringFlag{
						Name:  argStartJournalDir,
						Usage: "path to the journals database directory",
					},
					&cli.IntFlag{
						Name:  argMaxReadFDS,
						Usage: "maximum number of file desrcriptors used for read operations",
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
	ctx, cancel := context.WithCancel(context.Background())
	utils.NewNotifierOnIntTermSignal(func(s os.Signal) {
		getLogger().Warn("Handling signal=", s)
		cancel()
	})
	return server.Start(ctx, cfg)
}

func applyArgsToCfg(c *cli.Context, cfg *server.Config) {
	if hid := c.Int(argStartHostHostId); int(cfg.HostHostId) != hid {
		cfg.HostHostId = cluster.HostId(hid)
	}
	if jd := c.String(argStartJournalDir); jd != "" {
		cfg.JrnlCtrlConfig.JournalsDir = jd
	}
	if mfds := c.Int(argMaxReadFDS); mfds > 0 {
		cfg.JrnlCtrlConfig.MaxOpenFileDescs = mfds
	}
}

func getLogger() log4g.Logger {
	return log4g.GetLogger("logrange")
}
