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

	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/pkg/kv"
)

type (
	// HostRegistry interface allows to have access to all known cluster hosts
	HostRegistry interface {
		// Hosts returns map of all known hosts, including the current one
		Hosts(ctx context.Context) (HostsMap, error)

		// Lease returns the lease, acquired for the host registry
		Lease() kv.Lease
	}

	// HostsMap a map of HostId to HostInfo
	HostsMap map[cluster.HostId]HostInfo

	// HostInfo struct describes a Host information
	HostInfo struct {
		// Id is the cluster unique host identifier
		Id cluster.HostId

		// RpcAddr the Rpc Address the host can be connected by
		RpcAddr cluster.HostAddr
	}
)

// GetHostRegistry returns the HostRegistry object
func GetHostRegistry() HostRegistry {
	return getDAOs().hr
}
