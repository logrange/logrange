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

package cluster

import (
	"fmt"
	"strconv"
)

func init() {
	LocalHostRpcAddr = GetHostAddr("127.0.0.1", DefaultRpcPort)
}

type (
	// HostAddr is an address in host:port format
	HostAddr string

	// HostId is a type which keeps a host identifier
	HostId uint16
)

const (
	DefaultRpcPort = 9999
)

// HostId must be unique identifier in multi-host environment. The id could be
// used for generating some cross-cluster unique identifiers like chunk id etc.
// The value is assigned by data.Init() if it is not explicitly defined for the
// process when it is started
var HostId16 HostId

// ThisHostRPCAddress contains address of the logrange instance
var LocalHostRpcAddr HostAddr

// GetHostAddr gets an address (IP or domain) and adds the port number to it
func GetHostAddr(addr string, port int) HostAddr {
	return HostAddr(fmt.Sprintf("%s:%d", addr, port))
}

// String returns string by its HostId value
func (h HostId) String() string {
	return strconv.FormatInt(int64(h), 10)
}

// ParseHostId translates a string to HostId
func ParseHostId(hid string) (HostId, error) {
	res, err := strconv.ParseUint(hid, 10, 16)
	return HostId(res), err
}
