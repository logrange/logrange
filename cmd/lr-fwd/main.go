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
	"github.com/logrange/logrange/cmd"
	"github.com/logrange/logrange/forwarder"
	"github.com/logrange/logrange/forwarder/runner"
	"github.com/logrange/logrange/pkg/storage"
	"gopkg.in/urfave/cli.v2"
	"os"
	"sort"
)

const (
	Version = "0.1.0"
)

const (
	argStartCfgFile    = "config-file"
	argStartLogCfgFile = "log-config-file"
	argStartStorageDir = "storage-dir"
)

func main() {
	defer log4g.Shutdown()
	app := &cli.App{
		Name:    "lr-fwd",
		Version: Version,
		Usage:   "Log Forwarder",
		Commands: []*cli.Command{
			{
				Name:   "start",
				Usage:  "Run lr-fwd",
				Action: runForwarder,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  argStartLogCfgFile,
						Usage: "log4g configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartCfgFile,
						Usage: "lr-fwd configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartStorageDir,
						Usage: "lr-fwd storage directory",
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.FlagsByName(app.Commands[0].Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	if err := app.Run(os.Args); err != nil {
		getLogger().Fatal("Failed to run lr-fwd, cause: ", err)
	}
}

func runForwarder(c *cli.Context) error {
	logCfgFile := c.String(argStartLogCfgFile)
	if logCfgFile != "" {
		err := log4g.ConfigF(logCfgFile)
		if err != nil {
			return err
		}
	}

	logger := getLogger()
	cfg := forwarder.NewDefaultConfig()

	cfgFile := c.String(argStartCfgFile)
	if cfgFile != "" {
		logger.Info("Loading lr-fwd config from=", cfgFile)
		config, err := forwarder.LoadCfgFromFile(cfgFile)
		if err != nil {
			return err
		}
		cfg.Apply(config)
	}

	applyArgsToCfg(c, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cmd.NewNotifierOnIntTermSignal(func(s os.Signal) {
		logger.Warn("Handling signal=", s)
		cancel()
	})
	return runner.Run(ctx, cfg)
}

func applyArgsToCfg(c *cli.Context, cfg *forwarder.Config) {
	if sd := c.String(argStartStorageDir); sd != "" {
		cfg.Storage.Type = storage.TypeFile
		cfg.Storage.Location = sd
	}
}

func getLogger() log4g.Logger {
	return log4g.GetLogger("lr-fwd")
}
