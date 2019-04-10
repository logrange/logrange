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
		SrvQuerier  *ServerQuerier   `inject:""`
		SrvAdmin    *ServerAdmin     `inject:""`
		SrvStreams  *ServerStreams   `inject:""`
		rs          rrpc.Server
		ln          net.Listener
		logger      log4g.Logger
	}
)

// RPC endpoints
const (
	cRpcEpIngestorWrite  = 100
	cRpcEpQuerierQuery   = 200
	cRpcEpQuerierSources = 201
	cRpcEpAdminTruncate  = 300
	cRpcEpAdminExecute   = 301
	cRpcEpStreamsEnsure  = 400
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
	s.logger = log4g.GetLogger("api.pub.RpcServer").WithId("{" + s.ConnConfig.ListenAddr + "}").(log4g.Logger)
	s.rs = rrpc.NewServer()
	s.ln = l
	s.logger.Info("Initializing...")

	// register endpoints
	s.rs.Register(cRpcEpIngestorWrite, s.SrvIngestor.write)
	s.rs.Register(cRpcEpQuerierQuery, s.SrvQuerier.query)
	s.rs.Register(cRpcEpQuerierSources, s.SrvQuerier.sources)
	s.rs.Register(cRpcEpAdminTruncate, s.SrvAdmin.truncate)
	s.rs.Register(cRpcEpAdminExecute, s.SrvAdmin.execute)
	s.rs.Register(cRpcEpStreamsEnsure, s.SrvStreams.ensureStream)

	go s.listen()
	return nil
}

func (s *Server) Shutdown() {
	s.ln.Close()
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
