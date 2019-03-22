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
	"bufio"
	"context"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/client"
	"github.com/logrange/logrange/client/collector"
	"github.com/logrange/logrange/client/forwarder"
	"github.com/logrange/logrange/client/shell"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	ucli "gopkg.in/urfave/cli.v2"
	"os"
	"sort"
	"strings"
)

const (
	Version = "0.1.0"
)

const (
	argCfgFile    = "config-file"
	argLogCfgFile = "log-config-file"
	argServerAddr = "server-addr"
	argStorageDir = "storage-dir"

	argQueryStreamMode = "stream-mode"
)

var (
	cfg    = client.NewDefaultConfig()
	logger = log4g.GetLogger("lr")
)

func main() {
	defer log4g.Shutdown()

	cmnFlags := []ucli.Flag{
		&ucli.StringFlag{
			Name:  argServerAddr,
			Usage: "server address",
			Value: cfg.Transport.ListenAddr,
		},
		&ucli.StringFlag{
			Name:  argStorageDir,
			Usage: "storage directory",
		},
		&ucli.StringFlag{
			Name:  argCfgFile,
			Usage: "configuration file path",
		},
		&ucli.StringFlag{
			Name:  argLogCfgFile,
			Usage: "log4g configuration file path",
		},
	}

	app := &ucli.App{
		Name:    "lr",
		Version: Version,
		Usage:   "Logrange client",
		Commands: []*ucli.Command{
			{
				Name:   "collect",
				Usage:  "run data collection",
				Action: runCollector,
				Flags:  cmnFlags,
			},
			{
				Name:   "forward",
				Usage:  "run data forwarding",
				Action: runForwarder,
				Flags:  cmnFlags,
			},
			{
				Name:   "shell",
				Usage:  "run lql shell",
				Action: runShell,
				Flags:  []ucli.Flag{cmnFlags[0]},
			},
			{
				Name:      "query",
				Usage:     "execute lql query",
				Action:    execQuery,
				ArgsUsage: "[lql query]",
				Flags: []ucli.Flag{cmnFlags[0],
					&ucli.BoolFlag{
						Name:  argQueryStreamMode,
						Usage: "enable query stream mode (blocking)",
					},
				},
			},
		},
	}

	sort.Sort(ucli.FlagsByName(app.Flags))
	for _, cmd := range app.Commands {
		sort.Sort(ucli.FlagsByName(cmd.Flags))
	}

	if err := app.Run(os.Args); err != nil {
		logger.Error(err)
		if logger.GetLevel() == log4g.FATAL {
			fmt.Println(err)
		}
	}
}

func initCfg(c *ucli.Context) error {
	var (
		err error
	)

	logCfgFile := c.String(argLogCfgFile)
	if logCfgFile != "" {
		err = log4g.ConfigF(logCfgFile)
		if err != nil {
			return err
		}
	}

	cfgFile := c.String(argCfgFile)
	if cfgFile != "" {
		logger.Info("Loading config from=", cfgFile)
		config, err := client.LoadCfgFromFile(cfgFile)
		if err != nil {
			return err
		}
		cfg.Apply(config)
	}

	applyArgsToCfg(c, cfg)
	return nil
}

func applyArgsToCfg(c *ucli.Context, cfg *client.Config) {
	if sa := c.String(argServerAddr); sa != "" {
		cfg.Transport.ListenAddr = sa
	}
	if sd := c.String(argStorageDir); sd != "" {
		cfg.Storage.Type = storage.TypeFile
		cfg.Storage.Location = sd
	}
}

func newCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	utils.NewNotifierOnIntTermSignal(func(s os.Signal) {
		logger.Warn("Handling signal=", s)
		cancel()
	})
	return ctx
}

//===================== collector =====================

func runCollector(c *ucli.Context) error {
	err := initCfg(c)
	if err != nil {
		return err
	}

	cli, err := client.NewClient(*cfg.Transport)
	if err != nil {
		return err
	}

	defer cli.Close()
	strg, err := client.NewStorage(cfg.Storage)
	if err != nil {
		return err
	}

	return collector.Run(newCtx(), cfg.Collector, cli, strg)
}

//===================== forwarder =====================

func runForwarder(c *ucli.Context) error {
	err := initCfg(c)
	if err != nil {
		return err
	}

	cli, err := client.NewClient(*cfg.Transport)
	if err != nil {
		return err
	}

	defer cli.Close()
	strg, err := client.NewStorage(cfg.Storage)
	if err != nil {
		return err
	}

	return forwarder.Run(newCtx(), cfg, cli, strg)
}

//===================== query =====================

func execQuery(c *ucli.Context) error {
	log4g.SetLogLevel("", log4g.FATAL)
	err := initCfg(c)
	if err != nil {
		return err
	}

	query, err := getQuery(c)
	if err != nil {
		return err
	}

	cli, err := client.NewClient(*cfg.Transport)
	if err != nil {
		return err
	}

	defer cli.Close()
	return shell.Query(newCtx(), query, c.Bool(argQueryStreamMode), cli)
}

//===================== shell =====================

func runShell(c *ucli.Context) error {
	log4g.SetLogLevel("", log4g.FATAL)
	err := initCfg(c)
	if err != nil {
		return err
	}

	cli, err := client.NewClient(*cfg.Transport)
	if err != nil {
		return err
	}

	defer cli.Close()
	return shell.Run(cli)
}

func getQuery(c *ucli.Context) ([]string, error) {
	var (
		query []string
	)

	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 { //check if NOT file input
		if len(c.Args().Slice()) != 0 {
			query = append(query, strings.Join(c.Args().Slice(), " "))
			return query, nil
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() { //for now just read it all, later stream if needed
		t := strings.TrimSpace(scanner.Text())
		if t != "" {
			query = append(query, t)
		}
	}

	return query, scanner.Err()
}
