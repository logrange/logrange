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
	"encoding/json"
	"math/rand"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/pkg/kv"
	"github.com/pkg/errors"
)

type (
	// HostRegistry struct allows to have an access to all known cluster hosts
	HostRegistry struct {
		Strg kv.Storage         `inject:""`
		Cfg  HostRegistryConfig `inject:"HostRegistryConfig"`

		id     cluster.HostId
		logger log4g.Logger
		lease  kv.Lease

		// for testing purpose only
		retryTimeout time.Duration
	}

	// HostRegistryConfig interface is used for configuring new HostRegistry
	HostRegistryConfig interface {
		// HostId defines the cluster HostId to be registered. Value 0 means
		// it must be choosen to be unique
		HostId() cluster.HostId

		// Localhost defines params of the local host to be registered
		Localhost() HostInfo

		// LeaseTTL defines the lease timeout
		LeaseTTL() time.Duration

		// RegisterTimeout defines first register timeout 0 means forewer
		RegisterTimeout() time.Duration
	}

	// HostsMap a map of HostId to HostInfo
	HostsMap map[cluster.HostId]HostInfo

	// HostInfo struct describes a Host information
	HostInfo struct {
		// RpcAddr the Rpc Address the host can be connected by
		RpcAddr cluster.HostAddr `json:"rpcAddr"`
	}
)

func NewHostRegistry() *HostRegistry {
	hr := new(HostRegistry)
	hr.logger = log4g.GetLogger("cluster.model.HostRegistry")
	return hr
}

// Init as an implementation of linker.Initializer
func (hr *HostRegistry) Init(ctx context.Context) error {
	hr.logger.Info("Initializing...")
	cfg := hr.Cfg
	// get the lease first
	ls, err := hr.Strg.Lessor().NewLease(ctx, cfg.LeaseTTL(), true)
	if err != nil {
		return errors.Wrap(err, "Could not get the lease")
	}

	hi, err := json.Marshal(cfg.Localhost())
	if err != nil {
		ls.Release()
		return errors.Wrapf(err, "Could not marshal localhost: %s", cfg.Localhost())
	}

	hid := cfg.HostId()

	var endTime time.Time
	if cfg.RegisterTimeout() > 0 {
		endTime = time.Now().Add(cfg.RegisterTimeout())
	}

	retryTimeout := time.Second
	if hr.retryTimeout > 0 {
		retryTimeout = hr.retryTimeout
	}

	for endTime.IsZero() || endTime.After(time.Now()) {
		// look for a host Id if needed
		for hid == 0 {
			hid = cluster.HostId(rand.Int())
		}

		r := kv.Record{Key: makeKey(keyHostRegistryPfx, hid.String()), Value: hi, Lease: ls.Id()}

		_, err := hr.Strg.Create(ctx, r)
		if err == nil {
			hr.logger.Info("Could register the host with id=", hid)

			hr.id = hid
			hr.lease = ls
			return nil
		}

		if err == kv.ErrAlreadyExists {
			if cfg.HostId() == 0 {
				// no need to sleep, will try another Id
				hid = 0
				continue
			}
		}

		hr.logger.Warn("Could not register host (cluster.HostId16=", hid, ") in the storage, will sleep a second and try again, err=", err)
		select {
		case <-ctx.Done():
			// drop the lease
			ls.Release()
			return errors.Wrapf(ctx.Err(), "Could not register with id=%d, context is closed.", hid)
		case <-time.After(retryTimeout):
			// ok
		}
	}
	ls.Release()
	return errors.Errorf("Could not create HostRegistry, time is out")
}

// Shutdown provides an implementation of linker.Shutdowner
func (hr *HostRegistry) Shutdown() {
	hr.logger.Info("Shutdown...")
	hr.lease.Release()
}

// Id returns the cluster HostId which was choosen for the localhost
func (hr *HostRegistry) Id() cluster.HostId {
	return hr.id
}

// Hosts returns map of all known hosts, including the current one.
//
// Every registered host is represented by one record with keyHostRegistryPfx
// prefix and its own lease. This schema allows to re-register the host records
// when the host process is stopped
func (hr *HostRegistry) Hosts(ctx context.Context) (HostsMap, error) {
	hr.logger.Trace("Hosts() invoked")
	recs, err := hr.Strg.GetRange(ctx, kv.Key(keyHostRegistryPfx), kv.Key(keyHostRegistryLastPfx))
	if err != nil {
		return nil, errors.Wrap(err, "Could not receive Hosts range in the HostRegistry")
	}

	res := make(HostsMap, len(recs))
	for _, r := range recs {
		sid := getKeySuffix(r.Key, keyHostRegistryPfx)
		hid, err := cluster.ParseHostId(sid)
		if err != nil {
			hr.logger.Error("Read the key=", r.Key, " and could not parse suffix ", sid, " as its HostId. Skipping it. err=", err)
			continue
		}

		var val HostInfo
		err = json.Unmarshal(r.Value, &val)
		if err != nil {
			hr.logger.Error("Could not unmarshal value=", string(r.Value), " to HostInfo. hid=", hid, ". Skipping it. err=", err)
			continue
		}

		res[hid] = val
	}

	return res, nil
}

// Lease returns the lease, acquired for the host registry
func (hr *HostRegistry) Lease() kv.Lease {
	return hr.lease
}

// hrConfig is used in tests
type hrConfig struct {
	hostId          cluster.HostId
	localhost       HostInfo
	leaseTTL        time.Duration
	registerTimeout time.Duration
}

func (hrc *hrConfig) HostId() cluster.HostId {
	return hrc.hostId
}

func (hrc *hrConfig) Localhost() HostInfo {
	return hrc.localhost
}

func (hrc *hrConfig) LeaseTTL() time.Duration {
	return hrc.leaseTTL
}

func (hrc *hrConfig) RegisterTimeout() time.Duration {
	return hrc.registerTimeout
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}
