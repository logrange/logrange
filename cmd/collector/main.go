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
	"github.com/logrange/logrange/collector/storage"
	"gopkg.in/urfave/cli.v2"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/collector"
)

const (
	Version = "0.1.0"
)

const (
	argStartCfgFile     = "config-file"
	argStartLogCfgFile  = "log-config-file"
	argStartStorageFile = "storage-file"
)

func main() {
	defer log4g.Shutdown()
	app := &cli.App{
		Name:    "collector",
		Version: Version,
		Usage:   "Log Collector Agent",
		Commands: []*cli.Command{
			{
				Name:   "start",
				Usage:  "Run collector agent",
				Action: runCollector,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  argStartLogCfgFile,
						Usage: "log4g configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartCfgFile,
						Usage: "collector configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartStorageFile,
						Usage: "collector storage file path",
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.FlagsByName(app.Commands[0].Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	if err := app.Run(os.Args); err != nil {
		getLogger().Fatal("Failed to run collector, cause: ", err)
	}
}

func runCollector(c *cli.Context) error {
	logCfgFile := c.String(argStartLogCfgFile)
	if logCfgFile != "" {
		err := log4g.ConfigF(logCfgFile)
		if err != nil {
			return err
		}
	}

	logger := getLogger()
	cfg := collector.NewDefaultConfig()

	cfgFile := c.String(argStartCfgFile)
	if cfgFile != "" {
		logger.Info("Loading collector config from=", cfgFile)
		config, err := collector.LoadCfgFromFile(cfgFile)
		if err != nil {
			return err
		}
		cfg.Apply(config)
	}

	applyArgsToCfg(c, cfg)
	return collector.Run(cfg, ctxWithSignalHandler())
}

func applyArgsToCfg(c *cli.Context, cfg *collector.Config) {
	if sf := c.String(argStartStorageFile); sf != "" {
		cfg.Storage.Type = storage.TypeFile
		cfg.Storage.Location = sf
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
	return log4g.GetLogger("collector")
}
