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

package ctrlr

import (
	"sync"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cluster/model"
	lctx "github.com/logrange/logrange/pkg/context"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	advertiser struct {
		lock        sync.Mutex
		logger      log4g.Logger
		jrnlCatalog model.JournalCatalog
		clsdCh      chan struct{}
		closed      bool
		notifyCh    chan bool
		amap        map[string][]chunk.Id
	}
)

func newAdvertiser(jrnlCatalog model.JournalCatalog) *advertiser {
	a := new(advertiser)
	a.jrnlCatalog = jrnlCatalog
	a.logger = log4g.GetLogger("advertiser")
	a.amap = make(map[string][]chunk.Id)
	a.clsdCh = make(chan struct{})
	a.notifyCh = make(chan bool, 1)
	go a.reportChunks()
	return a
}

func (a *advertiser) advertise(jname string, ckIds []chunk.Id) {
	a.lock.Lock()
	if a.closed {
		a.lock.Unlock()
		a.logger.Warn("Advertising ", jname, ", after closing the component")
		return
	}
	notify := len(a.amap) == 0
	a.amap[jname] = ckIds
	a.lock.Unlock()
	if notify {
		a.notifyCh <- true
	}
}

func (a *advertiser) reportChunks() {
	a.logger.Info("reportChunks(): starting")
	ctx := lctx.WrapChannel(a.clsdCh)
	for {
		for {
			a.lock.Lock()
			u := a.amap
			if len(u) > 0 {
				a.amap = make(map[string][]chunk.Id)
			}
			a.lock.Unlock()

			if len(u) == 0 {
				a.logger.Debug("reportChunks(): nothing to report, waiting new notifications")
				break
			}

			// cleaning up notify channel
			select {
			case <-a.notifyCh:
			default:
			}

			for j, cids := range u {
				for ctx.Err() == nil {
					err := a.jrnlCatalog.ReportLocalChunks(ctx, j, cids)
					if err == nil {
						break
					}
				}
				if ctx.Err() != nil {
					return
				}
			}
		}

		select {
		case <-a.notifyCh:
		case <-a.clsdCh:
			a.logger.Info("reportChunks(): Closed now.")
			return
		}
	}
}

func (a *advertiser) close() {
	a.lock.Lock()
	if a.closed {
		a.lock.Unlock()
		a.logger.Warn("close(): already closed.")
		return
	}
	a.logger.Info("close() invoked")
	a.closed = true
	a.lock.Unlock()
	close(a.clsdCh)
}
