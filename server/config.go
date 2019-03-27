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

package server

import (
	"encoding/json"
	"fmt"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	"github.com/logrange/range/pkg/transport"
	"io/ioutil"
	"reflect"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/cluster"
	"github.com/logrange/range/pkg/cluster/model"
)

// Config struct defines logragnge server settings
type Config struct {

	// HostHostId defines the host unique identifier, if not set, then it will
	// be assigned automatically
	HostHostId cluster.HostId

	// HostLeaseTTLSec defines the Lease timeout in second, for registering
	// Host in the storage
	HostLeaseTTLSec int

	// HostRegisterTimeoutSec defines how long the host will try to register in
	// the storage until it is successfuly registered or stop. 0 value means
	// the timeout will be ignored
	HostRegisterTimeoutSec int

	// PublicApiRpc represents the transport configuration for public RPC API
	PublicApiRpc transport.Config

	// PrivateApiRpc represents the transport configuration for private RPC API
	PrivateApiRpc transport.Config

	// JrnlCtrlConfig an implementation of
	JrnlCtrlConfig JCtrlrConfig

	// NewTIndexOk shows whether the new tindex file could be created if it doesn't exist
	NewTIndexOk bool
}

type JCtrlrConfig struct {

	// JournalsDir the direcotry where journals are stored
	JournalsDir string

	// MaxOpenFileDescs defines how many file descriptors could be used
	// for read operations
	MaxOpenFileDescs int

	// CheckFullScan defines that the FullCheck() of IdxChecker will be
	// called. Normally LightCheck() is called only.
	CheckFullScan bool

	// RecoverDisabled flag defines that actual recover procedure should not
	// be run when the check chunk data test is failed.
	RecoverDisabled bool

	// RecoverLostDataOk flag defines that the chunk file could be truncated
	// if the file is partually corrupted.
	RecoverLostDataOk bool

	// WriteIdleSec specifies the writer idle timeout. It will be closed if no
	// write ops happens during the timeout
	WriteIdleSec int

	// WriteFlushMs specifies data sync timeout. Buffers will be synced after
	// last write operation.
	WriteFlushMs int

	// MaxChunkSize defines the maximum chunk size.
	MaxChunkSize int64

	// MaxRecordSize defines the maimum one record size
	MaxRecordSize int64
}

var configLog = log4g.GetLogger("Config")

func (c *Config) String() string {
	return fmt.Sprint(
		"\n\tHostHostId=", c.HostHostId,
		"\n\tHostLeaseTTLSec=", c.HostLeaseTTLSec,
		"\n\tHostRegisterTimeoutSec=", c.HostRegisterTimeoutSec,
		"\n\tPublicApiRpc=", c.PublicApiRpc,
		"\n\tPrivateApiRpc=", c.PrivateApiRpc,
		"\n\tJrnlCtrlConfig=", c.JrnlCtrlConfig,
		"\n\tNewTIndexOk=", c.NewTIndexOk,
	)
}

func GetDefaultConfig() *Config {
	c := new(Config)
	c.PublicApiRpc.ListenAddr = "127.0.0.1:9966"
	c.PrivateApiRpc.ListenAddr = "127.0.0.1:9967"
	c.HostLeaseTTLSec = 5
	c.JrnlCtrlConfig = GetDefaultJCtrlrConfig()
	return c
}

func GetDefaultJCtrlrConfig() JCtrlrConfig {
	jcc := JCtrlrConfig{}
	jcc.JournalsDir = "/opt/logrange/db/"
	jcc.MaxOpenFileDescs = 5000
	jcc.WriteIdleSec = 30
	jcc.WriteFlushMs = 500
	jcc.MaxChunkSize = 100 * 1024 * 1024
	jcc.MaxRecordSize = 1 * 1024 * 1024
	return jcc
}

// HostId is a part of model.HostRegistryConfig
func (c *Config) HostId() cluster.HostId {
	return c.HostHostId
}

// Localhost is a part of model.HostRegistryConfig
func (c *Config) Localhost() model.HostInfo {
	return model.HostInfo{RpcAddr: cluster.HostAddr(c.PrivateApiRpc.ListenAddr)}
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
	c.JrnlCtrlConfig.Apply(&cfg.JrnlCtrlConfig)
	if cfg.HostHostId > 0 {
		c.HostHostId = cfg.HostHostId
	}
	if !reflect.DeepEqual(c.PublicApiRpc, cfg.PublicApiRpc) {
		c.PublicApiRpc = cfg.PublicApiRpc
	}
	if !reflect.DeepEqual(c.PrivateApiRpc, cfg.PrivateApiRpc) {
		c.PrivateApiRpc = cfg.PrivateApiRpc
	}
	if cfg.HostLeaseTTLSec > 0 {
		c.HostLeaseTTLSec = cfg.HostLeaseTTLSec
	}
	if cfg.HostRegisterTimeoutSec > 0 {
		c.HostRegisterTimeoutSec = cfg.HostRegisterTimeoutSec
	}
}

func (c *JCtrlrConfig) Apply(cfg *JCtrlrConfig) {
	if cfg == nil {
		return
	}

	if len(cfg.JournalsDir) > 0 {
		c.JournalsDir = cfg.JournalsDir
	}
	if cfg.MaxOpenFileDescs > 0 {
		c.MaxOpenFileDescs = cfg.MaxOpenFileDescs
	}
	c.CheckFullScan = cfg.CheckFullScan
	c.RecoverDisabled = cfg.RecoverDisabled
	c.RecoverLostDataOk = cfg.RecoverLostDataOk
	if cfg.WriteIdleSec > 0 {
		c.WriteIdleSec = cfg.WriteIdleSec
	}
	if cfg.WriteFlushMs > 0 {
		c.WriteFlushMs = cfg.WriteFlushMs
	}
	if cfg.MaxChunkSize > 0 {
		c.MaxChunkSize = cfg.MaxChunkSize
	}
	if cfg.MaxRecordSize > 0 {
		c.MaxRecordSize = cfg.MaxRecordSize
	}
}

// FdPoolSize returns size of chunk.FdPool
func (c *JCtrlrConfig) FdPoolSize() int {
	fps := 100
	if c.MaxOpenFileDescs > 0 {
		fps = c.MaxOpenFileDescs
	}
	return fps
}

// StorageDir returns path to the dir on the local File system, where journals are stored
func (c *JCtrlrConfig) StorageDir() string {
	return c.JournalsDir
}

func (c JCtrlrConfig) String() string {
	return fmt.Sprint(
		"\n\t{\n\t\tJournalsDir=", c.JournalsDir,
		"\n\t\tMaxOpenFileDescs=", c.MaxOpenFileDescs,
		"\n\t\tCheckFullScan=", c.CheckFullScan,
		"\n\t\tRecoverDisabled=", c.RecoverDisabled,
		"\n\t\tRecoverLostDataOk=", c.RecoverLostDataOk,
		"\n\t\tWriteIdleSec=", c.WriteIdleSec,
		"\n\t\tWriteFlushMs=", c.WriteFlushMs,
		"\n\t\tMaxChunkSize=", c.MaxChunkSize,
		"\n\t\tMaxRecordSize=", c.MaxRecordSize,
		"\n\t}",
	)
}

// GetChunkConfig returns chunkfs.Config object, which will be used for constructing
// chunks
func (c *JCtrlrConfig) GetChunkConfig() chunkfs.Config {
	return chunkfs.Config{
		CheckFullScan:     c.CheckFullScan,
		RecoverDisabled:   c.RecoverDisabled,
		RecoverLostDataOk: c.RecoverLostDataOk,
		WriteIdleSec:      c.WriteIdleSec,
		WriteFlushMs:      c.WriteFlushMs,
		MaxChunkSize:      c.MaxChunkSize,
		MaxRecordSize:     c.MaxRecordSize,
	}
}

func ReadConfigFromFile(filename string) (*Config, error) {
	cfgData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	c := &Config{}
	err = json.Unmarshal(cfgData, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
