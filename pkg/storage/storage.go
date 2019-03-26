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

package storage

import (
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/utils"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

type (
	// Storage interface allows to read and write serialized data
	Storage interface {
		ReadData(key string) ([]byte, error)
		WriteData(key string, val []byte) error
	}

	// inmemStorage struct an in-mem Storage implementation
	inmemStorage struct {
		logger log4g.Logger
	}

	// fileStorage stuct a file Storage implementation
	fileStorage struct {
		location string
		logger   log4g.Logger
	}

	StorageType string
)

const (
	TypeFile  StorageType = "file"
	TypeInMem StorageType = "inmem"
)

var (
	inMemStore sync.Map
)

//===================== storage =====================

func NewStorage(cfg *Config) (Storage, error) {
	if err := cfg.Check(); err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}
	switch cfg.Type {
	case TypeFile:
		return newFileStorage(cfg.Location)
	case TypeInMem:
		return newInMemStorage(), nil
	}
	return nil, fmt.Errorf("unknown storage type=%v", cfg.Type)
}

func NewDefaultStorage() Storage {
	return newInMemStorage()
}

//===================== inmemStorage =====================

func newInMemStorage() Storage {
	logger := log4g.GetLogger("storage").WithId("[inmem]").(log4g.Logger)
	return &inmemStorage{logger: logger}
}

func (ms *inmemStorage) ReadData(key string) ([]byte, error) {
	v, ok := inMemStore.Load(key)
	if !ok {
		return nil, os.ErrNotExist
	}

	buf := v.([]byte)
	ms.logger.Debug("Read key=", key, ", value=", string(buf))
	return buf, nil
}

func (ms *inmemStorage) WriteData(key string, val []byte) error {
	if val == nil {
		return nil
	}

	buf := utils.BytesCopy(val)
	inMemStore.Store(key, buf)
	ms.logger.Debug("Wrote key=", key, ", value=", string(buf))
	return nil
}

func (ms *inmemStorage) String() string {
	return "[inmem]"
}

//===================== fileStorage =====================

func newFileStorage(location string) (Storage, error) {
	err := os.MkdirAll(location, 0740)
	if err != nil {
		return nil, err
	}

	logger := log4g.GetLogger("storage").WithId("[file]").(log4g.Logger)
	return &fileStorage{location: location, logger: logger}, nil
}

func (fs *fileStorage) ReadData(key string) ([]byte, error) {
	data, err := ioutil.ReadFile(fs.filePath(key))
	if os.IsNotExist(err) {
		return nil, os.ErrNotExist
	}
	if err == nil {
		fs.logger.Debug("Read key=", key, ", value=", string(data))
	}
	return data, err
}

func (fs *fileStorage) WriteData(key string, val []byte) error {
	err := ioutil.WriteFile(fs.filePath(key), val, 0640)
	if err == nil {
		fs.logger.Debug("Wrote key=", key, ", value=", string(val))
	}
	return err
}

func (fs *fileStorage) filePath(file string) string {
	return fmt.Sprintf("%v/%v", strings.TrimRight(fs.location, "/"), file)
}

func (fs *fileStorage) String() string {
	return fmt.Sprintf("[file: location=%v]", fs.location)
}
