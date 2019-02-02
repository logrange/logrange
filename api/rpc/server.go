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
	"context"
	"github.com/jrivets/log4g"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/transport"
	"github.com/pkg/errors"
	"net"
)

type (
	Server struct {
		ConnConfig  transport.Config `inject:"publicRpcTransport"`
		SrvIngestor *ServerIngestor  `inject:""`
		rs          rrpc.Server
		ln          net.Listener
		logger      log4g.Logger
	}
)

// RPC endpoints
const (
	cRpcEpIngestorWrite = 1
)

func NewServer() *Server {
	return new(Server)
}

// Init is part of Initializer interface
func (s *Server) Init(ctx context.Context) error {
	l, err := transport.NewServerListener(s.ConnConfig)
	if err != nil {
		return errors.Wrapf(err, "Could not create transport listener for %s", s.ConnConfig)
	}
	s.logger = log4g.GetLogger("rpcServer").WithId("{" + s.ConnConfig.ListenAddr + "}").(log4g.Logger)
	s.rs = rrpc.NewServer()
	s.ln = l

	// register endpoints
	s.rs.Register(cRpcEpIngestorWrite, s.SrvIngestor.ingestorWrite)

	go s.listen()
	return nil
}

func (s *Server) Shutdown() {
	s.rs.Close()
}

func (s *Server) listen() {
	s.logger.Info("listen(): start")
	defer s.logger.Info("listen(): stop")
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.logger.Warn("listen(): got the error when listen socket err=", err)
			return
		}

		err = s.rs.Serve(conn.RemoteAddr().String(), conn)
		if err != nil {
			s.logger.Warn("listen(): could not create new server connection for ", conn.RemoteAddr(), " err=", err)
			conn.Close()
		}
	}
}
