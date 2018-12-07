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

package server

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/pkg/cluster/model"
	"github.com/pkg/errors"
)

// Config struct defines logragnge server settings
type Config struct {
	// JournalsDir - contains path ont the local file-system where journals
	// data is stored
	JournalsDir string

	// HostHostId defines the host unique identifier, if not set, then it will
	// be assigned automatically
	HostHostId cluster.HostId

	// RpcAddress contains address for accessing peers via RPC
	HostRpcAddress cluster.HostAddr

	// HostLeaseTTLSec defines the Lease timeout in second, for registering
	// Host in the storage
	HostLeaseTTLSec int

	// HostRegisterTimeoutSec defines how long the host will try to register in
	// the storage until it is successfuly registered or stop. 0 value means
	// the timeout will be ignored
	HostRegisterTimeoutSec int
}

var configLog = log4g.GetLogger("Config")

func GetDefaultConfig() *Config {
	c := new(Config)
	c.JournalsDir = "/opt/logrange/db/"
	c.HostRpcAddress = "127.0.0.1:9966"
	c.HostLeaseTTLSec = 5
	return c
}

// HostId is a part of model.HostRegistryConfig
func (c *Config) HostId() cluster.HostId {
	return c.HostHostId
}

// Localhost is a part of model.HostRegistryConfig
func (c *Config) Localhost() model.HostInfo {
	return model.HostInfo{RpcAddr: c.HostRpcAddress}
}

// LeaseTTL is a part of model.HostRegistryConfig
func (c *Config) LeaseTTL() time.Duration {
	return time.Duration(c.HostLeaseTTLSec) * time.Second
}

// RegisterTimeout is a part of model.HostRegistryConfig
func (c *Config) RegisterTimeout() time.Duration {
	return time.Duration(c.HostRegisterTimeoutSec) * time.Second
}

// Apply override c's properties by non-default values from cfg
func (c *Config) Apply(cfg *Config) {
	if cfg == nil {
		return
	}
	if len(cfg.JournalsDir) > 0 {
		c.JournalsDir = cfg.JournalsDir
	}
	if cfg.HostHostId > 0 {
		c.HostHostId = cfg.HostHostId
	}
	if len(cfg.HostRpcAddress) > 0 {
		c.HostRpcAddress = cfg.HostRpcAddress
	}
	if cfg.HostLeaseTTLSec > 0 {
		c.HostLeaseTTLSec = cfg.HostLeaseTTLSec
	}
	if cfg.HostRegisterTimeoutSec > 0 {
		c.HostRegisterTimeoutSec = cfg.HostRegisterTimeoutSec
	}
}

// ReadConfigFromFile read config file from filename. It returns nil, if filename
// is empty or not found. It will panic if the file exists, but could not be
// read properly
func ReadConfigFromFile(filename string) *Config {
	if filename == "" {
		return nil
	}

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		configLog.Warn("There is no file ", filename, " for reading logrange config, will use default configuration.")
		return nil
	}

	cfgData, err := ioutil.ReadFile(filename)
	if err != nil {
		configLog.Fatal("Could not read configuration file ", filename, ": ", err)
		panic(errors.Wrapf(err, "Could not read data from config file %s", filename))
	}

	c := &Config{}
	err = json.Unmarshal(cfgData, c)
	if err != nil {
		configLog.Fatal("Could not unmarshal data from ", filename, ", err=", err)
		panic(errors.Wrapf(err, "Could not unmarshal json data from config file %s", filename))
	}

	configLog.Info("Configuration read from ", filename)
	return c
}
