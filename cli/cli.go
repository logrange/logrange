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

package cli

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/api/rpc"
	ucmd "github.com/logrange/logrange/cmd"
	"github.com/logrange/range/pkg/transport"
	"github.com/mohae/deepcopy"
	"github.com/peterh/liner"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

type (
	client struct {
		cfg *transport.Config
		rpc *rpc.Client
	}

	shell struct {
		cli   *client
		hfile string
	}
)

const (
	shellHistoryFileName = ".lr_history"
)

func Query(cfg *Config, ctx context.Context) error {
	cli, err := newClient(cfg.Transport)
	if err != nil {
		return err
	}
	if err := cli.connect(); err != nil {
		return err
	}

	err = selectFn(&config{
		query:  cfg.Query,
		stream: cfg.StreamMode,
		cli:    cli,
	}, ctx)

	if err != nil {
		printError(err)
	}

	_ = cli.close()
	return nil
}

func Shell(cfg *Config) error {
	cli, err := newClient(cfg.Transport)
	if err != nil {
		return err
	}
	if err := cli.connect(); err != nil {
		return err
	}
	newShell(cli, historyFilePath()).run()
	return nil
}

func historyFilePath() string {
	var fileDir = os.TempDir()
	usr, err := user.Current()
	if err == nil {
		fileDir = usr.HomeDir
	}
	return filepath.Join(fileDir, shellHistoryFileName)
}

func printError(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
}

//===================== shell =====================

func newShell(cli *client, hFile string) *shell {
	s := new(shell)
	s.cli = cli
	s.hfile = hFile
	return s
}

func (s *shell) run() {
	lnr := liner.NewLiner()
	lnr.SetCtrlCAborts(true)

	s.loadHistory(lnr)
	beforeQuit := func() {
		s.saveHistory(lnr)
		_ = lnr.Close()
		_ = s.cli.close()
		fmt.Println("bye!")
	}

	defer beforeQuit()
	cfg := &config{ //should be shared to allow setopts
		cli:        s.cli,
		beforeQuit: beforeQuit,
	}

	for {
		inp, err := lnr.Prompt("lr>")
		if err != nil {
			printError(err)
			if err == io.EOF || err == liner.ErrPromptAborted {
				break
			}
		}

		inp = strings.TrimSpace(inp)
		if inp == "" {
			continue
		}

		lnr.AppendHistory(inp)
		ctx, cancel := context.WithCancel(context.Background())
		ucmd.NewNotifierOnIntTermSignal(func(s os.Signal) {
			cancel()
		})

		err = execCmd(inp, cfg, ctx)
		if err != nil {
			printError(err)
		}
	}
}

func (s *shell) loadHistory(lnr *liner.State) {
	f, err := os.OpenFile(s.hfile, os.O_RDONLY|os.O_CREATE, 0640)
	if err != nil {
		printError(err)
		return
	}
	_, err = lnr.ReadHistory(f)
	if err != nil {
		printError(err)
		return
	}
	_ = f.Close()
}

func (s *shell) saveHistory(lnr *liner.State) {
	f, err := os.OpenFile(s.hfile, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		printError(err)
		return
	}
	_, err = lnr.WriteHistory(f)
	if err != nil {
		printError(err)
		return
	}
	_ = f.Close()
}

//===================== client =====================

func newClient(cfg *transport.Config) (*client, error) {
	if err := cfg.Check(); err != nil {
		return nil, err
	}

	cli := new(client)
	cli.cfg = deepcopy.Copy(cfg).(*transport.Config)
	return cli, nil
}

func (c *client) close() error {
	var err error
	if c.rpc != nil {
		err = c.rpc.Close()
		c.rpc = nil
	}
	return err
}

func (c *client) connect() error {
	var err error
	c.rpc, err = rpc.NewClient(*c.cfg)
	return err
}

func (c *client) doSelect(qr *api.QueryRequest, streamMode bool,
	handler func(res *api.QueryResult), ctx context.Context) error {

	limit := qr.Limit
	for ctx.Err() == nil {
		qr.Limit = limit

		res, err := c.query(ctx, qr)
		if err != nil {
			return err
		}

		if len(res.Events) != 0 {
			handler(res)
			qr = &res.NextQueryRequest
		}

		if !streamMode {
			if limit <= 0 || len(res.Events) == 0 {
				break
			}
			limit -= len(res.Events)
			continue
		}

		if len(res.Events) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}

	return nil
}

func (c *client) doDescribe(ctx context.Context, tc string) (*api.SourcesResult, error) {
	if c.rpc == nil {
		err := c.connect()
		if err != nil {
			return nil, err
		}
	}

	res := &api.SourcesResult{}
	err := c.rpc.Querier().Sources(ctx, tc, res)
	if err != nil {
		_ = c.close()
		return nil, err
	}
	return res, res.Err
}

func (c *client) query(ctx context.Context, qr *api.QueryRequest) (*api.QueryResult, error) {
	if c.rpc == nil {
		err := c.connect()
		if err != nil {
			return nil, err
		}
	}

	res := &api.QueryResult{}
	err := c.rpc.Querier().Query(ctx, qr, res)
	if err != nil {
		_ = c.close()
		return nil, err
	}

	return res, res.Err
}
