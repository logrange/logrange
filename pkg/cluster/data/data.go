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

package data

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/logrange/logrange/pkg/kv"
	"golang.org/x/sync/semaphore"
)

type Config struct {
	// Localhost defines params of the local host to be registered
	Localhost HostInfo

	// LeaseTTL defines the lease timeout
	LeaseTTL time.Duration

	// FailTimeout defines timeout for creating the new HostRegistry
	FailTimeout time.Duration
}

type daos struct {
	hr HostRegistry
}

var (
	dataSem = semaphore.NewWeighted(1)
	data    atomic.Value
)

// Init arrange all data resources and create all data access objects.
// It registers localhost and returns the HostRegistry. If the call
// is failed it indicates about a connectivity error or any issues by joining to the
// cluster.
//
// The function will try to register hrCfg.Localhost in the provided storage and
// create a lease for it. If the storage already has a record for the hostId, it will
// try to re-register, but no longer that for hrCfg.FailTimeout. The function
// will return HostRegistry object or an error if any.
//
// Returned HostRegistry must be closed when application is closed, to release the
// lease and makes it possible to remove all objects associated with the lease
func Init(ctx context.Context, storage kv.Storage, cfg Config) error {
	err := dataSem.Acquire(ctx, 1)
	if err != nil {
		return err
	}
	defer dataSem.Release(1)

	return nil
}

func Shutdown() {

}

func getDAOs() *daos {
	return data.Load().(*daos)
}
