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

package cursor

import (
	"context"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/container"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/logrange/pkg/utils"
	wpctx "github.com/logrange/range/pkg/context"
	"github.com/logrange/range/pkg/records/journal"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type (
	Provider struct {
		Tidx  tindex.Service     `inject:""`
		JCtrl journal.Controller `inject:""`

		logger     log4g.Logger
		lock       sync.Mutex
		busy       *container.CLElement
		free       *container.CLElement
		freePoolSz int
		curs       map[uint64]*container.CLElement
		clsdCh     chan struct{}

		maxCurs int
		idleTo  time.Duration
		// Timeout for cursors sweeping, which are acquired for read, but which were not released. Such
		// cursors could be removed when the timeout is fired.
		busyTo time.Duration
	}

	curHldr struct {
		busy    bool
		cur     *Cursor
		expTime time.Time
	}
)

func NewProvider() *Provider {
	p := new(Provider)
	p.curs = make(map[uint64]*container.CLElement)
	p.clsdCh = make(chan struct{})
	p.logger = log4g.GetLogger("cursor.Provider")
	p.maxCurs = 50000
	p.idleTo = time.Duration(60 * time.Second)
	p.busyTo = time.Duration(5 * time.Minute)
	return p
}

// Init for implementing linker.Initializer
func (p *Provider) Init(ctx context.Context) error {
	go p.sweeper()
	return nil
}

// Shutdown is a part o linker.Shutdowner
func (p *Provider) Shutdown() {
	close(p.clsdCh)
}

// GetOrCreate gets existing or creates a new cursor. if state.Id is 0, the new cursor will be always created.
// If the cursor is returned it must be released explicitly by Release() to be garbage collected properly
func (p *Provider) GetOrCreate(ctx context.Context, state State) (*Cursor, error) {
	var res *Cursor
	if state.Id > 0 {
		p.lock.Lock()
		if e, ok := p.curs[state.Id]; ok {
			ch := e.Val.(*curHldr)
			if ch.busy {
				p.lock.Unlock()
				return nil, errors.Errorf("Cursor usage violation: concurrent request for id=%d", state.Id)
			}

			if err := ch.cur.ApplyState(state); err != nil {
				p.logger.Warn("Could not apply state ", state, " to the cursor ", ch.cur, ". Will try to create the new one. err=", err)
				state.Id = 0
			} else {
				ch.busy = true
				p.busy = p.busy.TearOff(e)
				p.busy = e.Append(p.busy)
				res = ch.cur
				ch.expTime = time.Now().Add(p.busyTo)
			}
		}
		p.lock.Unlock()
	}

	if res != nil {
		return res, nil
	}

	if state.Id == 0 {
		state.Id = utils.NextSimpleId()
	}

	cur, err := newCursor(ctx, state, p.Tidx, p.JCtrl)
	if err != nil {
		return nil, err
	}

	p.lock.Lock()
	e := p.free
	if e != nil {
		p.free = p.free.TearOff(e)
		p.freePoolSz--
	} else {
		e = container.NewCLElement()
		e.Val = &curHldr{}
	}
	ch := e.Val.(*curHldr)
	ch.busy = true
	ch.cur = cur
	ch.expTime = time.Now().Add(p.busyTo)
	p.busy = e.Append(p.busy)
	p.curs[cur.Id()] = e
	p.lock.Unlock()

	return cur, nil
}

func (p *Provider) Release(ctx context.Context, cur *Cursor) State {
	res := cur.commit(ctx)
	p.lock.Lock()
	e, ok := p.curs[cur.Id()]
	if !ok {
		p.lock.Unlock()
		p.logger.Debug("Releasing cursor, which is not in the cache anymore: ", cur)
		return res
	}
	ch := e.Val.(*curHldr)
	if !ch.busy {
		p.lock.Unlock()
		panic("incorrect usage - releasing cursor, which is not busy")
	}

	ch.busy = false
	ch.expTime = time.Now().Add(p.idleTo)
	p.busy = p.busy.TearOff(e)
	p.busy = e.Append(p.busy)
	p.lock.Unlock()
	return res
}

func (p *Provider) sweeper() {
	p.logger.Info("sweeper(): starting")
	defer p.logger.Info("sweeper(): over")

	ctx := wpctx.WrapChannel(p.clsdCh)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.idleTo / 5):
		}

		p.lock.Lock()
		p.sweepBySize()
		p.sweepByTime()
		p.lock.Unlock()
	}
}

func (p *Provider) sweepBySize() {
	cnt := 0
	for len(p.curs) > p.maxCurs {
		e := p.busy.Prev()
		ch := e.Val.(*curHldr)
		delete(p.curs, ch.cur.Id())
		p.busy = p.busy.TearOff(e)
		cnt++
	}
	if cnt > 0 {
		p.logger.Warn(cnt, " records were removed due to oversize. p.maxCurs=", p.maxCurs)
	}
}

func (p *Provider) sweepByTime() {
	now := time.Now()
	e := p.busy
	cnt := len(p.curs)
	rmvd := 0
	for e != nil && cnt > 0 {
		e = e.Prev()
		ch := e.Val.(*curHldr)
		if ch.expTime.Before(now) {
			if ch.busy {
				p.logger.Warn("Removing cursor ", ch.cur, " due to timeout, but it is not returned yet!")
			}
			e1 := e.Next()
			p.busy = p.busy.TearOff(e)
			delete(p.curs, ch.cur.Id())
			ch.cur = nil
			if p.freePoolSz < 1000 {
				p.free = e.Append(p.free)
				p.freePoolSz++
			}
			e = e1
			rmvd++
		} else if !ch.busy {
			return
		}
		cnt--
	}
	if rmvd > 0 {
		p.logger.Debug(rmvd, " cursors were deleted due to their timeouts.")
	}
}
