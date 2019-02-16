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

package logevent

import (
	"github.com/logrange/logrange/pkg/container"
	"github.com/logrange/range/pkg/cluster"
	"sync"
	"sync/atomic"
	"time"
)

type TagIdGenerator struct {
	lock    sync.Mutex
	knwnIds *container.CLElement
	ids     map[string]*container.CLElement
	max     int
}

type tigVal struct {
	ti   uint64
	tags string
}

var lastTid uint64

func init() {
	lastTid = (uint64(time.Now().UnixNano()) & 0xFFFFFFFFFFFF0000) | uint64(cluster.HostId16&0xFFFF)
}

func newTagId() uint64 {
	for {
		ltid := atomic.LoadUint64(&lastTid)
		tid := ltid + 0x10000
		if atomic.CompareAndSwapUint64(&lastTid, ltid, tid) {
			return tid
		}
	}
}

func NewTagIdGenerator(maxSize int) *TagIdGenerator {
	tig := new(TagIdGenerator)
	tig.ids = make(map[string]*container.CLElement)
	tig.max = maxSize
	return tig
}

func (tig *TagIdGenerator) GetOrCreateId(k string) uint64 {
	tig.lock.Lock()
	if e, ok := tig.ids[k]; ok {
		tig.knwnIds = tig.knwnIds.TearOff(e)
		tig.knwnIds = e.Append(tig.knwnIds)
		v := e.Val.(tigVal)
		tig.lock.Unlock()
		return v.ti
	}

	var e *container.CLElement
	if len(tig.ids) == tig.max {
		e = tig.knwnIds.Prev()
		tig.knwnIds = tig.knwnIds.TearOff(e)
		v := e.Val.(tigVal)
		delete(tig.ids, v.tags)
	} else {
		e = container.NewCLElement()
	}

	res := newTagId()
	e.Val = tigVal{res, k}
	tig.knwnIds = e.Append(tig.knwnIds)
	tig.ids[k] = e
	tig.lock.Unlock()
	return res
}
