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
	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/server"
	"github.com/pkg/errors"
	"gopkg.in/urfave/cli.v2"
)

const (
	Version = "0.1.0"
)

const (
	// Common flag names
	argLogCfgFile = "log-config-file"
	argCfgFile    = "config-file"

	// Start command flag names
	argStartHostHostId     = "host-id"
	argStartHostRPCAddr    = "host-rpc-address"
	argStartHostLeaseTTL   = "host-lease-ttl"
	argStartHostRegTimeout = "host-registration-timeout"
	argStartJournalDir     = "journals-dir"
)

var log = log4g.GetLogger("logrange")
var cfg = server.GetDefaultConfig()

func main() {
	defer log4g.Shutdown()

	app := &cli.App{
		Name:    "logrange",
		Version: Version,
		Usage:   "Log Aggregation service",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  argLogCfgFile,
				Usage: "The log4g configuration file name",
				Value: "/opt/logrange/log4g.properties",
			},
			&cli.StringFlag{
				Name:  argCfgFile,
				Usage: "The logrange configuration file name",
				Value: "/opt/logrange/config.json",
			},
		},
		Before: before,
		Commands: []*cli.Command{
			&cli.Command{
				Name:   "start",
				Usage:  "Run the service",
				Action: runServer,
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:  argStartHostHostId,
						Usage: "Unique host identifier, if 0 the id will be automatically assigned.",
						Value: int(cfg.HostHostId),
					},
					&cli.StringFlag{
						Name:  argStartHostRPCAddr,
						Usage: "Advertised RPC address. Peers in the cluster will use it for connecting to the host",
						Value: string(cfg.HostRpcAddress),
					},
					&cli.IntFlag{
						Name:  argStartHostLeaseTTL,
						Usage: "Lease TTL in seconds. Used in cluster config",
						Value: int(cfg.HostLeaseTTLSec),
					},
					&cli.IntFlag{
						Name:  argStartHostRegTimeout,
						Usage: "Host registration timeout in seconds. 0 means forewer.",
						Value: int(cfg.HostRegisterTimeoutSec),
					},
					&cli.StringFlag{
						Name:  argStartJournalDir,
						Usage: "Defines path to the journals database directory",
						Value: cfg.JournalsDir,
					},
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.FlagsByName(app.Commands[0].Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Run(os.Args)
}

func before(c *cli.Context) error {
	logCfgFile := c.String(argLogCfgFile)
	if logCfgFile != "" {
		if _, err := os.Stat(logCfgFile); os.IsNotExist(err) {
			log.Warn("No file ", logCfgFile, " will use default log4g configuration")
		} else {
			log.Info("Loading log4g config from ", logCfgFile)
			err := log4g.ConfigF(logCfgFile)
			if err != nil {
				err := errors.Wrapf(err, "Could not parse %s file as a log4g configuration, please check syntax ", logCfgFile)
				log.Fatal(err)
				return err
			}
		}
	}

	fc := server.ReadConfigFromFile(c.String(argCfgFile))
	if fc != nil {
		// overwrite default settings from file
		cfg.Apply(fc)
	}

	return nil
}

func runServer(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-sigChan:
			log.Info("Got signal \"", s, "\", cancelling context ")
			cancel()
		}
	}()

	// fill up config
	applyParamsToCfg(c)
	return server.Start(ctx, cfg)
}

func applyParamsToCfg(c *cli.Context) {
	dc := server.GetDefaultConfig()
	if hid := c.Int(argStartHostHostId); int(dc.HostHostId) != hid {
		cfg.HostHostId = cluster.HostId(hid)
	}
	if hra := c.String(argStartHostRPCAddr); dc.HostRpcAddress != cluster.HostAddr(hra) {
		cfg.HostRpcAddress = cluster.HostAddr(hra)
	}
	if lttl := c.Int(argStartHostLeaseTTL); int(dc.HostLeaseTTLSec) != lttl {
		cfg.HostLeaseTTLSec = lttl
	}
	if hrt := c.Int(argStartHostRegTimeout); int(dc.HostRegisterTimeoutSec) != hrt {
		cfg.HostRegisterTimeoutSec = hrt
	}
	if jd := c.String(argStartJournalDir); dc.JournalsDir != jd {
		cfg.JournalsDir = jd
	}
}
