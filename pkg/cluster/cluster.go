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

package cluster

import (
	"fmt"
)

func init() {
	LocalHostRpcAddr = GetHostAddr("127.0.0.1", DefaultRpcPort)
	HostId16 = DefaultHostId
}

type (
	// HostAddr is an address in host:port format
	HostAddr string

	// HostId is a type which keeps a host identifier
	HostId uint16
)

const (
	DefaultRpcPort = 9999
	DefaultHostId  = 0
)

// HostId must be unique identifier in multi-host environment. The id could be
// used for generating some cross-cluster unique identifiers like chunk id etc.
// The value cannot be DefaultHostId in distributed environment
var HostId16 HostId

// ThisHostRPCAddress contains address of the logrange instance
var LocalHostRpcAddr HostAddr

func GetHostAddr(addr string, port int) HostAddr {
	return HostAddr(fmt.Sprintf("%s:%d", addr, port))
}
