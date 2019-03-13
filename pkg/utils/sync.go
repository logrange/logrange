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
	"context"
	"sync"
	"time"
)

func Wait(ctx context.Context, ticker *time.Ticker) bool {
	select {
	case <-ctx.Done():
		return false
	case <-ticker.C:
		return true
	}
}

func Sleep(ctx context.Context, t time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(t):
		return true
	}
}

func WaitDone(done chan bool, t time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(t):
		return false
	}
}

func WaitWaitGroup(wg *sync.WaitGroup, t time.Duration) bool {
	done := make(chan bool)
	go func() {
		wg.Wait()
		close(done)
	}()
	return WaitDone(done, t)
}
