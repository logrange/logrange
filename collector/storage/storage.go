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

package storage

import (
	"fmt"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/collector/utils"
	"io/ioutil"
	"os"
)

type (
	// Storage interface allows to read and write serialized data
	Storage interface {
		ReadData() ([]byte, error)
		WriteData(buf []byte) error
	}

	// inmemStorage struct an in-mem Storage implementation
	inmemStorage struct {
		buf    []byte
		logger log4g.Logger
	}

	// fileStorage stuct a file Storage implementation
	fileStorage struct {
		fileName string
		logger   log4g.Logger
	}

	StorageType string
)

const (
	TypeFile  StorageType = "file"
	TypeInMem StorageType = "inmem"
)

//===================== storage =====================

func NewStorage(cfg *Config) (Storage, error) {
	if err := cfg.Check(); err != nil {
		return nil, err
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
	s, _ := NewStorage(NewDefaultConfig())
	return s
}

//===================== inmemStorage =====================

func newInMemStorage() Storage {
	logger := log4g.GetLogger("collector.storage").WithId("[inmem]").(log4g.Logger)
	return &inmemStorage{buf: make([]byte, 0), logger: logger}
}

func (ms *inmemStorage) ReadData() ([]byte, error) {
	if ms.buf == nil {
		return nil, os.ErrNotExist
	}
	ms.logger.Debug("Read data=", string(ms.buf))
	return ms.buf, nil
}

func (ms *inmemStorage) WriteData(buf []byte) error {
	if buf == nil {
		ms.buf = nil
		return nil
	}
	ms.buf = utils.BytesCopy(buf)
	ms.logger.Debug("Wrote data=", string(ms.buf))
	return nil
}

func (ms *inmemStorage) String() string {
	return fmt.Sprintf("[inmem]")
}

//===================== fileStorage =====================

func newFileStorage(fn string) (Storage, error) {
	if err := openOrCreate(fn); err != nil {
		return nil, err
	}

	logger := log4g.GetLogger("collector.storage").WithId("[file]").(log4g.Logger)
	return &fileStorage{fileName: fn, logger: logger}, nil
}

func (fs *fileStorage) ReadData() ([]byte, error) {
	data, err := ioutil.ReadFile(fs.fileName)
	if err == nil {
		fs.logger.Debug("Read data=", string(data))
	}
	return data, err
}

func (fs *fileStorage) WriteData(buf []byte) error {
	err := ioutil.WriteFile(fs.fileName, buf, 0640)
	if err == nil {
		fs.logger.Debug("Wrote data=", string(buf))
	}
	return err
}

func (fs *fileStorage) String() string {
	return fmt.Sprintf("[file:%v]", fs.fileName)
}

//===================== utils =====================

func openOrCreate(fn string) error {
	file, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0640)
	_ = file.Close()
	return err
}
