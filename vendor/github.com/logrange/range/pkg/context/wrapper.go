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

package context

import (
	ctx "context"
	"fmt"
	"time"
)

type (
	closingCtx struct {
		ch <-chan struct{}
	}
)

// WrapChannel receives a channel and returns a context which wraps the channel
// The context will be closed when the channel is closed.
func WrapChannel(ch <-chan struct{}) ctx.Context {
	return &closingCtx{ch}
}

func (cc *closingCtx) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (cc *closingCtx) Done() <-chan struct{} {
	return cc.ch
}

func (cc *closingCtx) Err() error {
	select {
	case _, ok := <-cc.ch:
		if ok {
			panic("Improper use of the the context wrapper")
		}
		return fmt.Errorf("The underlying channel was closed")
	default:
		return nil
	}
}

func (cc *closingCtx) Value(key interface{}) interface{} {
	return nil
}
