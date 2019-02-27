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

package rpc

import (
	"github.com/logrange/logrange/api"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/transport"
)

type (
	// Client is rpc client which provides the API interface for clients
	Client struct {
		rc     rrpc.Client
		cing   *clntIngestor
		cqrier *clntQuerier
	}
)

// NewClient creates new Client for connecting to the server, using the transport config tcfg
func NewClient(tcfg transport.Config) (*Client, error) {
	conn, err := transport.NewClientConn(tcfg)
	if err != nil {
		return nil, err
	}
	c := new(Client)
	c.rc = rrpc.NewClient(conn)
	c.cing = new(clntIngestor)
	c.cing.rc = c.rc
	c.cqrier = new(clntQuerier)
	c.cqrier.rc = c.rc
	return c, nil
}

// Ingestor return api.Ingestor interface for the client
func (c *Client) Ingestor() api.Ingestor {
	return c.cing
}

// Ingestor return api.Querier interface for the client
func (c *Client) Querier() api.Querier {
	return c.cqrier
}
