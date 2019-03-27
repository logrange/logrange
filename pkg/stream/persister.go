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

package stream

import (
	"encoding/json"
	"github.com/jrivets/log4g"
	"github.com/logrange/range/pkg/utils/fileutil"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"path"
)

type (
	streamPersister struct {
		dir    string
		logger log4g.Logger
	}
)

const (
	cStreamsFileName = "streams.dat"
)

func newStreamPersister(dir string) *streamPersister {
	sp := new(streamPersister)
	sp.dir = dir
	sp.logger = log4g.GetLogger("stream.persister")
	return sp
}

func (sp *streamPersister) loadStreams() ([]Stream, error) {
	fn := path.Join(sp.dir, cStreamsFileName)
	sp.logger.Info("Loading list of streams from ", fn)
	_, err := os.Stat(fn)
	if os.IsNotExist(err) {
		sp.logger.Warn(" file ", fn, " doesn't exist. Returning empty list ")
		return nil, nil
	}

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		sp.logger.Warn("could not unmarshal context from file ", fn, " data size ", len(data), " err=", err)
		return nil, errors.Wrapf(err, "could not read file %s ", fn)
	}

	var res []Stream
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse data from the file %s, seems like corrupted data...", fn)
	}

	sp.logger.Info("Read information about ", len(res), " streams")
	return res, nil
}

func (sp *streamPersister) saveStreams(strms []Stream) error {
	fn := path.Join(sp.dir, cStreamsFileName)
	data, err := json.Marshal(strms)
	if err != nil {
		return errors.Wrapf(err, "could not marshal strms ")
	}

	if err = ioutil.WriteFile(fn, data, 0640); err != nil {
		return errors.Wrapf(err, "could not write file %s ", fn)
	}

	return nil
}

func (sp *streamPersister) streamFileName(name string) string {
	efn := fileutil.EscapeToFileName(name)
	return path.Join(sp.dir, "stream"+efn+".dat")
}

func (sp *streamPersister) loadStreamInfo(name string, res map[string]*ssDesc) error {
	fn := sp.streamFileName(name)
	_, err := os.Stat(fn)
	if os.IsNotExist(err) {
		return nil
	}

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		sp.logger.Warn("loadStreamInfo(): could not unmarshal context from file ", fn, " data size ", len(data), " err=", err)
		return errors.Wrapf(err, "could not read file %s ", fn)
	}

	err = json.Unmarshal(data, &res)
	if err != nil {
		return errors.Wrapf(err, "loadStreamInfo(): could not parse data from the file %s, seems like corrupted data...", fn)
	}

	return nil
}

func (sp *streamPersister) saveStreamInfo(name string, info map[string]*ssDesc) error {
	fn := sp.streamFileName(name)
	data, err := json.Marshal(info)
	if err != nil {
		return errors.Wrapf(err, "could not marshal info for stream %s ", name)
	}

	if err = ioutil.WriteFile(fn, data, 0640); err != nil {
		return errors.Wrapf(err, "saveStreamInfo(): could not write file %s ", fn)
	}

	return nil
}

func (sp *streamPersister) onDeleteStream(name string) {
	fn := sp.streamFileName(name)
	if err := os.Remove(fn); err != nil {
		sp.logger.Error("onDeleteStream(): could not delete file ", fn, ", for the name=", name, ", err=", err)
	}
}
