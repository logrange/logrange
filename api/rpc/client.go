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
	"time"
)

type (
	// Client is rpc client which provides the API interface for clients
	Client struct {
		rc      rrpc.Client
		cfg     transport.Config
		cing    *clntIngestor
		cqrier  *clntQuerier
		admin   *clntAdmin
		streams *clntStreams
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
	var err error
	if c.rc != nil {
		err = c.rc.Close()
		c.rc = nil
	}
	return err
}

func (c *Client) connect() error {
	if c.rc != nil {
		_ = c.Close()
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
		time.Sleep(time.Second)
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
	c.streams = new(clntStreams)
	c.streams.rc = c.rc
	return nil
}

func (c *Client) Sources(ctx context.Context, tc string, res *api.SourcesResult) error {
	if c.rc == nil {
		err := c.connect()
		if err != nil {
			return err
		}
	}

	err := c.cqrier.Sources(ctx, tc, res)
	if err != nil {
		_ = c.Close()
		return err
	}
	return res.Err
}

func (c *Client) Truncate(ctx context.Context, request api.TruncateRequest) (api.TruncateResult, error) {
	if c.rc == nil {
		err := c.connect()
		if err != nil {
			return api.TruncateResult{}, err
		}
	}

	res, err := c.admin.Truncate(ctx, request)
	if err != nil {
		_ = c.Close()
		return api.TruncateResult{}, err
	}

	return res, nil
}

func (c *Client) Query(ctx context.Context, qr *api.QueryRequest, res *api.QueryResult) error {
	if c.rc == nil {
		err := c.connect()
		if err != nil {
			return err
		}
	}

	err := c.cqrier.Query(ctx, qr, res)
	if err != nil {
		_ = c.Close()
		return err
	}

	return res.Err
}

func (c *Client) Write(ctx context.Context, tags, fields string, evs []*api.LogEvent, res *api.WriteResult) error {
	if c.rc == nil {
		err := c.connect()
		if err != nil {
			return err
		}
	}

	err := c.cing.Write(ctx, tags, fields, evs, res)
	if err != nil {
		_ = c.Close()
		return err
	}

	return res.Err
}

func (c *Client) EnsureStream(ctx context.Context, stm api.Stream, res *api.StreamCreateResult) error {
	if c.rc == nil {
		err := c.connect()
		if err != nil {
			return err
		}
	}

	err := c.streams.EnsureStream(ctx, stm, res)
	if err != nil {
		_ = c.Close()
	}

	return err
}
