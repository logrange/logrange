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

package model

import (
	"context"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/pkg/kv/inmem"
)

func newHR() *HostRegistry {
	ms := inmem.New()
	hr := NewHostRegistry()
	hr.Strg = ms
	hr.Cfg = &hrConfig{}
	return hr
}

func TestNewHostRegistry(t *testing.T) {
	hr := newHR()
	err := hr.Init(context.Background())
	if err == nil {
		t.Fatal("Must fail on registering new lease, but err=nil")
	}

	// create first host
	hr.Cfg.(*hrConfig).leaseTTL = time.Second
	err = hr.Init(context.Background())
	if err != nil {
		t.Fatal("Must be able to be registered, but err=", err)
	}

	// re-register hr2, waiting 1st is releasing
	hr.retryTimeout = time.Millisecond
	hr.Cfg.(*hrConfig).hostId = hr.Id()

	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		hr.Shutdown()
	}()
	err = hr.Init(context.Background())
	if err != nil {
		t.Fatal("Must be able to be registered, but err=", err)
	}
	if time.Now().Sub(start) < 5*time.Millisecond {
		t.Fatal("Must be looping in new, but returns immediately")
	}

	// re-register, but ctx will be cancelled
	start = time.Now()
	cctx, _ := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err = hr.Init(cctx)
	if err == nil {
		t.Fatal("Must not be able to complete, but err=", err)
	}
	if time.Now().Sub(start) < 5*time.Millisecond {
		t.Fatal("Must be looping in new, but returns immediately")
	}

}

func TestHostRegistryHosts(t *testing.T) {
	hr1 := newHR()

	hr1Addr := cluster.GetHostAddr("hr", 111)
	hr1.Cfg.(*hrConfig).localhost.RpcAddr = hr1Addr
	hr1.Cfg.(*hrConfig).leaseTTL = time.Second
	err := hr1.Init(context.Background())
	if err != nil {
		t.Fatal("Must be able to be registered, but err=", err)
	}

	hr2Addr := cluster.GetHostAddr("hr2", 222)
	hr2 := NewHostRegistry()
	hr2.Cfg = &hrConfig{}
	*(hr2.Cfg.(*hrConfig)) = *(hr1.Cfg.(*hrConfig))
	hr2.Cfg.(*hrConfig).localhost.RpcAddr = hr2Addr
	hr2.Strg = hr1.Strg
	err = hr2.Init(context.Background())
	if err != nil {
		t.Fatal("Must be able to be registered, but err=", err)
	}

	hm, err := hr1.Hosts(context.Background())
	if err != nil || len(hm) != 2 {
		t.Fatal("expecting no error with len(hm) == 2, but err=", err, " hm=", hm)
	}

	if hm[hr1.Id()].RpcAddr != hr1Addr || hm[hr2.Id()].RpcAddr != hr2Addr {
		t.Fatal("Wrong context hm=", hm, " hr1=", hr1, " hr2=", hr2)
	}

	hr2.Lease().Release()
	hm, err = hr1.Hosts(context.Background())
	if err != nil || len(hm) != 1 {
		t.Fatal("expecting no error with len(hm) == 1, but err=", err, " hm=", hm)
	}

	if hm[hr1.Id()].RpcAddr != hr1Addr {
		t.Fatal("Wrong context hm=", hm, " hr1=", hr1)
	}

}
