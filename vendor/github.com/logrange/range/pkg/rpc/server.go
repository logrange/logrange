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
	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"github.com/logrange/range/pkg/utils/errors"
	"io"
	"sync"
	"sync/atomic"
)

type (
	server struct {
		logger log4g.Logger

		lock    sync.Mutex
		bufPool *bytes.Pool
		closed  bool
		funcs   atomic.Value
		conns   map[interface{}]*ServerConn
	}

	ServerConn struct {
		lock   sync.Mutex
		srvr   *server
		codec  *srvIOCodec
		logger log4g.Logger
		closed int32
		wLock  sync.Mutex
	}
)

func NewServer() *server {
	s := new(server)
	s.logger = log4g.GetLogger("rpc.server")
	s.funcs.Store(make(map[int16]OnClientReqFunc))
	s.conns = make(map[interface{}]*ServerConn)
	s.bufPool = new(bytes.Pool)
	return s
}

func (s *server) Register(funcId int, cb OnClientReqFunc) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return errors.ClosedState
	}

	funcs := s.funcs.Load().(map[int16]OnClientReqFunc)
	newFncs := make(map[int16]OnClientReqFunc)
	for k, v := range funcs {
		newFncs[k] = v
	}
	newFncs[int16(funcId)] = cb

	s.funcs.Store(newFncs)
	return nil
}

func (s *server) Serve(codecId string, rwc io.ReadWriteCloser) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return errors.ClosedState
	}

	scodec := newSrvIOCodec(codecId, rwc)

	sc := new(ServerConn)
	sc.logger = log4g.GetLogger("rpc.ServerConn").WithId("{" + scodec.id + "}").(log4g.Logger)
	sc.codec = scodec
	sc.srvr = s
	s.conns[sc] = sc
	go sc.readLoop()
	return nil
}

func (s *server) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return errors.ClosedState
	}
	s.logger.Info("Close()")
	s.closed = true
	conns := s.conns
	go func() {
		for _, sc := range conns {
			sc.Close()
		}
	}()

	s.conns = nil
	return nil
}

func (s *server) onClose(sc *ServerConn) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return
	}

	delete(s.conns, sc)
}

func (s *server) call(funcId int16, reqId int32, reqBody []byte, sc *ServerConn) {
	funcs := s.funcs.Load().(map[int16]OnClientReqFunc)
	if f, ok := funcs[funcId]; ok {
		f(reqId, reqBody, sc)
		return
	}
	s.logger.Warn("Got request ", reqId, " for unregistered function ", funcId, "skiping it")
	s.bufPool.Release(reqBody)
}

func (sc *ServerConn) Collect(buf []byte) {
	sc.srvr.bufPool.Release(buf)
}

// SendResponse allows to send the response by the request id
func (sc *ServerConn) SendResponse(reqId int32, opErr error, msg xbinary.Writable) {
	if atomic.LoadInt32(&sc.closed) != 0 {
		return
	}

	sc.wLock.Lock()
	err := sc.codec.writeResponse(reqId, opErr, msg)
	sc.wLock.Unlock()

	if err != nil {
		sc.logger.Warn("Could not write response for reqId=", reqId, ", opErr=", opErr)
		sc.closeByError(err)
	}
}

func (sc *ServerConn) Close() error {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	if atomic.LoadInt32(&sc.closed) != 0 {
		return errors.ClosedState
	}

	sc.logger.Info("Close()")
	sc.codec.Close()
	atomic.StoreInt32(&sc.closed, 1)
	sc.srvr.onClose(sc)
	return nil
}

func (sc *ServerConn) closeByError(err error) {
	sc.logger.Warn("closeByError(): err=", err)
	sc.Close()
}

func (sc *ServerConn) readLoop() {
	sc.logger.Info("readLoop(): starting")
	defer sc.logger.Info("readLoop(): ending")
	for atomic.LoadInt32(&sc.closed) == 0 {
		funcId, reqId, body, err := sc.readFromWire()
		if err != nil {
			sc.closeByError(err)
			return
		}
		sc.srvr.call(funcId, reqId, body, sc)
	}
}

func (sc *ServerConn) readFromWire() (funcId int16, reqId int32, body []byte, err error) {
	var bsz int
	reqId, funcId, bsz, err = sc.codec.readRequest()
	if err != nil {
		return
	}

	body = sc.srvr.bufPool.Arrange(bsz)
	err = sc.codec.readRequestBody(body)
	if err != nil {
		sc.srvr.bufPool.Release(body)
		body = nil
	}
	return
}
