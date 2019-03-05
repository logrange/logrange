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

package remote

import (
	"context"
	"errors"
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/api/rpc"
	"github.com/logrange/logrange/collector/model"
	"github.com/logrange/logrange/collector/utils"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/mohae/deepcopy"
	"time"
	"unsafe"
)

type (
	Client struct {
		cfg *Config
		rpc *rpc.Client

		done   chan bool
		logger log4g.Logger
	}
)

//===================== client =====================

func NewClient(cfg *Config) (*Client, error) {
	if err := cfg.Check(); err != nil {
		return nil, err
	}

	cli := new(Client)
	cli.cfg = deepcopy.Copy(cfg).(*Config)
	cli.logger = log4g.GetLogger("remote")
	return cli, nil
}

func (c *Client) Run(ctx context.Context, events <-chan *model.Event) error {
	err := c.connect(ctx)
	if err != nil {
		return err
	}

	c.runSend(ctx, events)
	return nil
}

func (c *Client) Close() error {
	var err error
	if !utils.WaitDone(c.done, time.Minute) {
		err = errors.New("close timeout")
	}
	if c.rpc != nil {
		err = c.rpc.Close()
	}
	c.logger.Info("Closed, err=", err)
	return err
}

func (c *Client) runSend(ctx context.Context, events <-chan *model.Event) {
	c.done = make(chan bool)
	go func() {
		defer close(c.done)

		var (
			err     error
			sendRes api.WriteResult
		)

		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				for ctx.Err() == nil {
					if err == nil {
						if err = c.send(ctx, ev, &sendRes); err == nil {
							if sendRes.Err != nil {
								c.logger.Warn("Tried to deliver event=", ev,
									", but server returned write err=", sendRes.Err)
							}
							ev.Confirm()
							break
						}
					}

					c.logger.Info("Communication error, recovering in ",
						c.cfg.ConnectRetryIntervalSec, "sec; cause: ", err)
					utils.Sleep(ctx, time.Duration(c.cfg.ConnectRetryIntervalSec)*time.Second)
					err = c.connect(ctx)
				}
			}
		}
	}()
}

func (c *Client) connect(ctx context.Context) error {
	var (
		err error
		try int
	)

	c.logger.Info("Connecting to ", c.cfg.Transport.ListenAddr)
	retry := time.Duration(c.cfg.ConnectRetryIntervalSec) * time.Second

	for try < c.cfg.ConnectMaxRetry {
		c.rpc, err = rpc.NewClient(*c.cfg.Transport)
		if err == nil {
			c.logger.Info("Connected!")
			break
		}

		try++
		c.logger.Warn("Connection error (attempt: ", try,
			" of ", c.cfg.ConnectMaxRetry, "), err=", err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		case <-time.After(retry):
		}
	}

	return err
}

func (c *Client) send(ctx context.Context, ev *model.Event, sendRes *api.WriteResult) error {
	if c.rpc == nil {
		return fmt.Errorf("rpc not initialized")
	}

	set := tag.MapToSet(ev.Meta.Tags)

	c.logger.Debug("Sending event=", ev)
	return c.rpc.Ingestor().Write(ctx, string(set.Line()), toApiEvents(ev), sendRes)
}

func toApiEvents(ev *model.Event) []*api.LogEvent {
	res := make([]*api.LogEvent, 0, len(ev.Records))
	for _, r := range ev.Records {
		res = append(res, &api.LogEvent{
			Timestamp: uint64(r.Date.UnixNano()),
			Message:   *(*string)(unsafe.Pointer(&r.Data)),
		})
	}
	return res
}
