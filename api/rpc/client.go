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

package rpc

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/api"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/transport"
	"net"
	"sync"
)

type (
	// Client is rpc client which provides the API interface for clients
	Client struct {
		lock   sync.Mutex
		rc     rrpc.Client
		cfg    transport.Config
		cing   *clntIngestor
		cqrier *clntQuerier
		admin  *clntAdmin
		pipes  *clntPipes
	}
)

// NewClient creates new Client for connecting to the server, using the transport config tcfg
func NewClient(tcfg transport.Config) (*Client, error) {
	if err := tcfg.Check(); err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}

	c := new(Client)
	c.cfg = tcfg

	err := c.connect()
	return c, err
}

func (c *Client) Close() error {
	c.lock.Lock()
	var err error
	if c.rc != nil {
		err = c.rc.Close()
		c.rc = nil
	}
	c.cing = nil
	c.cqrier = nil
	c.admin = nil
	c.pipes = nil
	c.lock.Unlock()
	return err
}

func (c *Client) connect() error {
	if c.rc != nil {
		return nil
	}

	var (
		conn net.Conn
		err  error
	)

	maxRetry := 3
	for {
		maxRetry--
		conn, err = transport.NewClientConn(c.cfg)
		if err == nil || maxRetry <= 0 {
			break
		}
	}

	if err != nil {
		return err
	}

	c.rc = rrpc.NewClient(conn)
	c.cing = new(clntIngestor)
	c.cing.rc = c.rc
	c.cqrier = new(clntQuerier)
	c.cqrier.rc = c.rc
	c.admin = new(clntAdmin)
	c.admin.rc = c.rc
	c.pipes = new(clntPipes)
	c.pipes.rc = c.rc
	return nil
}

func (c *Client) Execute(ctx context.Context, req api.ExecRequest) (api.ExecResult, error) {
	c.lock.Lock()
	if err := c.connect(); err != nil {
		c.lock.Unlock()
		return api.ExecResult{}, err
	}
	a := c.admin
	c.lock.Unlock()

	res, err := a.Execute(ctx, req)
	if err != nil {
		_ = c.Close()
		return api.ExecResult{}, err
	}

	return res, nil
}

func (c *Client) Query(ctx context.Context, qr *api.QueryRequest, res *api.QueryResult) error {
	c.lock.Lock()
	if err := c.connect(); err != nil {
		c.lock.Unlock()
		return err
	}
	q := c.cqrier
	c.lock.Unlock()

	err := q.Query(ctx, qr, res)
	if err != nil {
		_ = c.Close()
		return err
	}

	return res.Err
}

func (c *Client) Write(ctx context.Context, tags, fields string, evs []*api.LogEvent, res *api.WriteResult) error {
	c.lock.Lock()
	if err := c.connect(); err != nil {
		c.lock.Unlock()
		return err
	}
	ci := c.cing
	c.lock.Unlock()

	err := ci.Write(ctx, tags, fields, evs, res)
	if err != nil {
		_ = c.Close()
		return err
	}

	return res.Err
}

func (c *Client) EnsurePipe(ctx context.Context, p api.Pipe, res *api.PipeCreateResult) error {
	c.lock.Lock()
	if err := c.connect(); err != nil {
		c.lock.Unlock()
		return err
	}
	pps := c.pipes
	c.lock.Unlock()

	err := pps.EnsurePipe(ctx, p, res)
	if err != nil {
		_ = c.Close()
	}

	return err
}
