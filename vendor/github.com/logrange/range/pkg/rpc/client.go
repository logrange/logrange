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
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	lerrors "github.com/logrange/range/pkg/utils/errors"
	"github.com/pkg/errors"
	"io"
	"sync"
	"sync/atomic"
)

type (
	client struct {
		logger  log4g.Logger
		bufPool *bytes.Pool

		lock      sync.Mutex
		calls     map[int32]*call
		freeCalls *call
		closed    bool

		wLock sync.Mutex
		codec *clntIOCodec
	}

	resp struct {
		opErr error
		body  []byte
	}

	call struct {
		sigCh chan resp
		next  *call
	}
)

var reqId int32

// NewClient creates new Client object for building RPC over rwc
func NewClient(rwc io.ReadWriteCloser) Client {
	c := new(client)
	c.logger = log4g.GetLogger("rpc.client")
	c.calls = make(map[int32]*call)
	c.codec = newClntIOCodec(rwc)
	go c.readLoop()
	return c
}

// Call allows to make synchronous RPC call to server side
func (c *client) Call(ctx context.Context, funcId int, m xbinary.Writable) (respBody []byte, opErr error, err error) {
	rid := atomic.AddInt32(&reqId, 1)
	c.lock.Lock()
	if c.closed {
		c.lock.Unlock()
		c.logger.Warn("Call(): invoked for closed client: funcId=", funcId)
		err = lerrors.ClosedState
		return
	}
	cl := c.arrangeCall()
	c.calls[rid] = cl
	c.lock.Unlock()

	c.wLock.Lock()
	err = c.codec.writeRequest(rid, int16(funcId), m)
	c.wLock.Unlock()

	if err != nil {
		c.closeByError(err)
		err = errors.Wrapf(err, "Call(): c.codec.writeRequest() for funcId=%d, rid=%d", funcId, rid)
		return
	}

	select {
	case sResp, ok := <-cl.sigCh:
		if !ok {
			c.logger.Warn("Call(): signal channel was closed")
			err = lerrors.ClosedState
			return
		}
		respBody = sResp.body
		opErr = sResp.opErr
	case <-ctx.Done():
		c.logger.Debug("Call(): ctx is closed before receiving result")
		err = ctx.Err()
	}

	c.lock.Lock()
	// either way it was signalled, or not - removing the call from the list
	delete(c.calls, rid)
	c.releaseCall(cl)
	c.lock.Unlock()
	return
}

// Collect allows to put buf to the Pool of buffers
func (c *client) Collect(buf []byte) {
	c.bufPool.Release(buf)
}

// Close closes the Client
func (c *client) Close() error {
	c.logger.Info("Close()")
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.close()
}

func (c *client) isClosed() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.closed
}

func (c *client) close() error {
	if c.closed {
		return lerrors.ClosedState
	}

	c.closed = true
	for _, cl := range c.calls {
		close(cl.sigCh)
	}

	for c.freeCalls != nil {
		cl := c.freeCalls
		close(cl.sigCh)
		c.freeCalls = cl.next
		cl.next = nil
	}
	c.calls = nil
	return c.codec.Close()
}

func (c *client) closeByError(err error) {
	if err == nil {
		return
	}
	c.logger.Error("closeByError(): err=", err)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.close()
}

func (c *client) readLoop() {
	c.logger.Info("readLoop(): starting.")
	defer c.logger.Info("readLoop(): ending.")
	for {
		reqId, opErr, bodySize, err := c.codec.readResponse()
		if err != nil {
			c.closeByError(err)
			return
		}

		buf := c.bufPool.Arrange(bodySize)
		err = c.codec.readResponseBody(buf)
		if err != nil {
			c.closeByError(err)
			c.bufPool.Release(buf)
			return
		}

		c.lock.Lock()
		if c.closed {
			c.lock.Unlock()
			c.bufPool.Release(buf)
			c.logger.Info("readLoop(): closed state detected")
			return
		}

		if cl, ok := c.calls[reqId]; ok {
			resp := resp{opErr: opErr, body: buf}
			cl.sigCh <- resp
			// remove the call from the list as already signaled...
			delete(c.calls, reqId)
		} else {
			c.bufPool.Release(buf)
		}
		c.lock.Unlock()
	}
}

func (c *client) arrangeCall() *call {
	if c.freeCalls == nil {
		cl := new(call)
		cl.sigCh = make(chan resp, 1)
		return cl
	}

	res := c.freeCalls
	c.freeCalls = res.next
	res.next = nil
	return res
}

func (c *client) releaseCall(cl *call) {
	select {
	case sResp, ok := <-cl.sigCh:
		if ok {
			c.bufPool.Release(sResp.body)
		}
	default:
	}
	cl.next = c.freeCalls
	c.freeCalls = cl.next
}
