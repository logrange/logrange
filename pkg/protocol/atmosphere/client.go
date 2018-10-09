package atmosphere

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/jrivets/log4g"
)

type (
	client struct {
		cfg       ClientConfig
		strm      stream
		lock      sync.Mutex
		cond      *sync.Cond
		state     int
		writers   int
		sessId    string
		ctxCancel context.CancelFunc
		ctx       context.Context
		logger    log4g.Logger
	}
)

const (
	csAuthenticating = iota
	csReady
	csWriting
	csClosed
)

func newClient(ccfg *ClientConfig, rwc io.ReadWriteCloser) *client {
	c := new(client)
	c.cfg = *ccfg
	c.cond = sync.NewCond(&c.lock)
	c.state = csAuthenticating
	c.logger = log4g.GetLogger("atmosphere.client")
	c.ctx, c.ctxCancel = context.WithCancel(context.Background())
	c.strm.rwc = rwc
	c.strm.logger = log4g.GetLogger("atmosphere.client.strm")
	go c.authenticate()
	return c
}

func (c *client) Write(m Message, rm Message) (int, error) {
	err := c.acquireWriter()
	if err != nil {
		return 0, err
	}

	err = c.strm.writeBuf(ptMessage, m)
	if err != nil {
		c.Close()
		c.releaseWriter()
		return 0, err
	}

	tp, n, err := c.strm.readHeader()
	if err != nil || tp != ptMessage {
		c.Close()
		c.releaseWriter()
		return 0, err
	}

	err = c.strm.readOrSkip(n, rm)
	c.releaseWriter()
	return n, err
}

func (c *client) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	if c.state != csClosed {
		c.logger.Info("Closing the client")
		c.state = csClosed
		c.ctxCancel()
		c.cond.Broadcast()
		err = c.strm.close()
	}
	return err
}

// ------------------------- the client internals ----------------------------
func (c *client) authenticate() {
	c.logger.Info("Authenticating...")
	ar, err := c.strm.authHandshake(&AuthReq{AccessKey: strPtr(c.cfg.AccessKey), SecretKey: strPtr(c.cfg.SecretKey)})
	if err != nil {
		c.logger.Error("authenticate(): Could not write auth request, err=", err)
		c.Close()
		return
	}

	c.logger.Info("authenticate(): Response ", ar.String())
	sid := strFromPtr(ar.SessionId)
	if ar.Terminal || len(sid) == 0 {
		c.logger.Warn("authenticate(): Got terminal response, or empty session id")
		c.Close()
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	if c.state != csAuthenticating {
		c.logger.Warn("Could not complete authentication, state is changed ")
		return
	}

	c.state = csReady
	c.sessId = strFromPtr(ar.SessionId)
	c.logger.Debug("Switching to csReady c.writers=", c.writers)
	if c.writers > 0 {
		c.cond.Signal()
	}

	go c.heartBeat()
}

// getSessionId gets current session id, for test purposes only
func (c *client) getSessionId() string {
	c.acquireWriter()
	defer c.releaseWriter()
	return c.sessId
}

func (c *client) heartBeat() {
	to := time.Duration(c.cfg.HeartBeatMs) * time.Millisecond
	if to == 0 {
		c.logger.Warn("No heartbeat. Timeout is 0")
		return
	}
	c.logger.Info("Will send heartbeat every ", c.cfg.HeartBeatMs, "ms")
	defer c.logger.Info("Heartbeat is over")
	for {
		select {
		case <-time.After(to):
			if !c.sendHeartBeat() {
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *client) sendHeartBeat() bool {
	err := c.acquireWriter()
	if err != nil {
		c.Close()
		return false
	}
	defer c.releaseWriter()

	ar, err := c.strm.authHandshake(&AuthReq{SessionId: strPtr(c.sessId)})
	if err != nil {
		c.Close()
		return false
	}

	c.logger.Debug("sendHeartBeat(): Response ", &ar)
	sid := strFromPtr(ar.SessionId)
	if ar.Terminal {
		c.logger.Warn("sendHeartBeat(): Got terminal response ", &ar)
		c.Close()
		return false
	}

	c.sessId = sid
	return true
}

func (c *client) acquireWriter() error {
	c.logger.Trace("acquireWriter()")
	c.lock.Lock()
	for c.state == csWriting || c.state == csAuthenticating {
		c.writers++
		c.cond.Wait()
		c.writers--
	}
	c.logger.Trace("acquireWriter(): awaken")

	var err error
	if c.state == csReady {
		c.state = csWriting
	} else {
		err = ErrCannotWrite
	}
	c.lock.Unlock()
	return err
}

func (c *client) releaseWriter() {
	c.lock.Lock()
	if c.state == csWriting {
		if c.writers > 0 {
			c.cond.Signal()
		}
		c.state = csReady
	}
	c.lock.Unlock()
}
