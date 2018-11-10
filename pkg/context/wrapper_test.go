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

package context

import (
	"testing"
	"time"
)

func TestWrapperNil(t *testing.T) {
	ctx := WrapChannel(nil)
	select {
	case <-ctx.Done():
		t.Fatal("Must not be here")
	default:
	}

	if ctx.Err() != nil {
		t.Fatal("must be nil")
	}
}

func TestWrapperClosedChannel(t *testing.T) {
	ch := make(chan struct{})
	close(ch)
	ctx := WrapChannel(ch)
	select {
	case <-ctx.Done():
	//ok
	default:
		t.Fatal("Must not be here")
	}

	if ctx.Err() == nil {
		t.Fatal("must not be nil")
	}
}

func TestWrapperNormal(t *testing.T) {
	ch := make(chan struct{})
	ctx := WrapChannel(ch)
	if ctx.Err() != nil {
		t.Fatal("must be nil")
	}
	start := time.Now()
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ch)
	}()
	select {
	case <-ctx.Done():
	}

	if time.Now().Sub(start) < time.Microsecond*10 {
		t.Fatal("sthould be 10ms at least")
	}
	if ctx.Err() == nil {
		t.Fatal("must be nil")
	}
}
