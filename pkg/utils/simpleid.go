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

package utils

import (
	"github.com/logrange/range/pkg/cluster"
	"sync/atomic"
	"time"
)

var lastTid uint64

func init() {
	lastTid = (uint64(time.Now().UnixNano()) & 0xFFFFFFFFFFFF0000) | uint64(cluster.HostId16&0xFFFF)
}

func NextSimpleId() uint64 {
	for {
		ltid := atomic.LoadUint64(&lastTid)
		tid := ltid + 0x10000
		if atomic.CompareAndSwapUint64(&lastTid, ltid, tid) {
			return tid
		}
	}
}
