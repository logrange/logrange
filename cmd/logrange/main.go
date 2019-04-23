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
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange"
	cmd2 "github.com/logrange/logrange/cmd"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/logrange/server"
	"github.com/logrange/range/pkg/cluster"
	"github.com/logrange/range/pkg/utils/fileutil"
	"gopkg.in/urfave/cli.v2"
	"os"
	"path"
	"sort"
)

const (
	argStartLogCfgFile = "log-config-file"
	argStartCfgFile    = "config-file"
	argStartHostHostId = "host-id"
	argStartBaseDir    = "base-dir"
	argStartAsDaemon   = "daemon"
	argMaxReadFDS      = "max-read-fds"
)

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
						Name:  argStartBaseDir,
						Usage: "path to the directory, where logrange data will be stored",
					},
					&cli.IntFlag{
						Name:  argMaxReadFDS,
						Usage: "maximum number of file desrcriptors used for read operations",
					},
					&cli.BoolFlag{
						Name:  argStartAsDaemon,
						Usage: "starting the server as daemon (detached from the console)",
					},
				},
			},
			{
				Name:   "stop",
				Usage:  "Run logrange service, if it is started",
				Action: stopServer,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  argStartCfgFile,
						Usage: "server configuration file path",
					},
					&cli.StringFlag{
						Name:  argStartBaseDir,
						Usage: "path to the directory, where logrange data is stored",
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.FlagsByName(app.Commands[0].Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
	}
}

func getServerConfig(c *cli.Context) (*server.Config, error) {
	var cfg = server.GetDefaultConfig()
	logCfgFile := c.String(argStartLogCfgFile)
	if logCfgFile != "" {
		err := log4g.ConfigF(logCfgFile)
		if err != nil {
			return nil, err
		}
	}

	cfgFile := c.String(argStartCfgFile)
	if cfgFile != "" {
		fmt.Println("Loading server config from=", cfgFile)
		config, err := server.ReadConfigFromFile(cfgFile)
		if err != nil {
			return nil, err
		}
		cfg.Apply(config)
	}

	applyArgsToCfg(c, cfg)
	return cfg, nil
}

func runServer(c *cli.Context) error {
	cfg, err := getServerConfig(c)
	if err != nil {
		return err
	}

	if c.Bool(argStartAsDaemon) {
		res := cmd2.RemoveArgsWithName(os.Args[1:], argStartAsDaemon)
		return cmd2.RunCommand(os.Args[0], res...)
	}

	err = fileutil.EnsureDirExists(cfg.BaseDir)
	if err != nil {
		return fmt.Errorf("could not create dir %s err=%s", cfg.BaseDir, err)
	}

	pf := cmd2.NewPidFile(pidFileName(cfg))
	if !pf.Lock() {
		return fmt.Errorf("already running?")
	}
	defer pf.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	utils.NewNotifierOnIntTermSignal(func(s os.Signal) {
		fmt.Println(" Handling signal=", s)
		cancel()
	})

	return server.Start(ctx, cfg)
}

func stopServer(c *cli.Context) error {
	cfg, err := getServerConfig(c)
	if err != nil {
		return err
	}

	pf := cmd2.NewPidFile(pidFileName(cfg))
	return pf.Interrupt()
}

func applyArgsToCfg(c *cli.Context, cfg *server.Config) {
	if hid := c.Int(argStartHostHostId); int(cfg.HostHostId) != hid {
		cfg.HostHostId = cluster.HostId(hid)
	}
	if jd := c.String(argStartBaseDir); jd != "" {
		cfg.BaseDir = jd
	}
	if mfds := c.Int(argMaxReadFDS); mfds > 0 {
		cfg.JrnlCtrlConfig.MaxOpenFileDescs = mfds
	}
}

func pidFileName(cfg *server.Config) string {
	return path.Join(cfg.BaseDir, "logrange.pid")
}
