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
	"github.com/logrange/logrange/cli"
	"github.com/logrange/logrange/cmd"
	ucli "gopkg.in/urfave/cli.v2"
	"os"
	"sort"
	"strings"
)

const (
	Version = "0.1.0"
)

const (
	argServerAddr      = "server-addr"
	argQueryStreamMode = "stream-mode"
)

var cfg = cli.NewDefaultConfig()

func main() {
	defer log4g.Shutdown()
	app := &ucli.App{
		Name:    "lr",
		Version: Version,
		Usage:   "Logrange Command Line Interface",
		Action:  shell,
		Commands: []*ucli.Command{
			{
				Name:      "query",
				Usage:     "execute Lql query",
				Action:    query,
				ArgsUsage: "[Lql query]",
				Flags: []ucli.Flag{
					&ucli.StringFlag{
						Name:  argServerAddr,
						Usage: "logrange server address",
						Value: cfg.Transport.ListenAddr,
					},
					&ucli.BoolFlag{
						Name:  argQueryStreamMode,
						Usage: "enable query stream mode (blocking)",
					},
				},
			},
			{
				Name:   "shell",
				Usage:  "run Lql shell",
				Action: shell,
				Flags: []ucli.Flag{
					&ucli.StringFlag{
						Name:  argServerAddr,
						Usage: "logrange server address",
						Value: cfg.Transport.ListenAddr,
					},
				},
			},
		},
	}

	sort.Sort(ucli.FlagsByName(app.Flags))
	sort.Sort(ucli.FlagsByName(app.Commands[0].Flags))
	sort.Sort(ucli.FlagsByName(app.Commands[1].Flags))

	log4g.SetLogLevel("", log4g.FATAL)
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

func query(c *ucli.Context) error {
	if err := applyQuery(c); err != nil {
		return err
	}
	applyArgsToCfg(c, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cmd.NewNotifierOnIntTermSignal(func(s os.Signal) {
		cancel()
	})
	return cli.Query(ctx, cfg)
}

func shell(c *ucli.Context) error {
	applyArgsToCfg(c, cfg)
	return cli.Shell(cfg)
}

func applyQuery(c *ucli.Context) error {
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 { //check if NOT file input
		if len(c.Args().Slice()) != 0 {
			cfg.Query = append(cfg.Query, strings.Join(c.Args().Slice(), ""))
			return nil
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() { //for now just read it all, later stream if needed
		t := strings.TrimSpace(scanner.Text())
		if t != "" {
			cfg.Query = append(cfg.Query, t)
		}
	}
	return scanner.Err()
}

func applyArgsToCfg(c *ucli.Context, cfg *cli.Config) {
	if sa := c.String(argServerAddr); sa != "" {
		cfg.Transport.ListenAddr = sa
	}
	if sm := c.Bool(argQueryStreamMode); sm != cfg.StreamMode {
		cfg.StreamMode = sm
	}
}
