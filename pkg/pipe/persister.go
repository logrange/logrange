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

package pipe

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
	persister struct {
		dir    string
		logger log4g.Logger
	}
)

const (
	cPipesFileName = "pipes.dat"
)

func newPersister(dir string) *persister {
	sp := new(persister)
	sp.dir = dir
	sp.logger = log4g.GetLogger("pipe.persister")
	return sp
}

func (sp *persister) loadPipes() ([]Pipe, error) {
	fn := path.Join(sp.dir, cPipesFileName)
	sp.logger.Info("Loading list of pipes from ", fn)
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

	var res []Pipe
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse data from the file %s, seems like corrupted data...", fn)
	}

	sp.logger.Info("Read information about ", len(res), " streams")
	return res, nil
}

func (sp *persister) savePipes(pps []Pipe) error {
	fn := path.Join(sp.dir, cPipesFileName)
	data, err := json.Marshal(pps)
	if err != nil {
		return errors.Wrapf(err, "could not marshal ppipes ")
	}

	if err = ioutil.WriteFile(fn, data, 0640); err != nil {
		return errors.Wrapf(err, "could not write file %s ", fn)
	}

	return nil
}

func (sp *persister) pipeFileName(name string) string {
	efn := fileutil.EscapeToFileName(name)
	return path.Join(sp.dir, "pipe"+efn+".dat")
}

func (sp *persister) loadPipeInfo(name string, res map[string]*ppDesc) error {
	fn := sp.pipeFileName(name)
	_, err := os.Stat(fn)
	if os.IsNotExist(err) {
		return nil
	}

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		sp.logger.Warn("loadPipeInfo(): could not unmarshal context from file ", fn, " data size ", len(data), " err=", err)
		return errors.Wrapf(err, "could not read file %s ", fn)
	}

	err = json.Unmarshal(data, &res)
	if err != nil {
		return errors.Wrapf(err, "loadPipeInfo(): could not parse data from the file %s, seems like corrupted data...", fn)
	}

	return nil
}

func (sp *persister) savePipeInfo(name string, info map[string]*ppDesc) error {
	fn := sp.pipeFileName(name)
	data, err := json.Marshal(info)
	if err != nil {
		return errors.Wrapf(err, "could not marshal info for pipe %s ", name)
	}

	if err = ioutil.WriteFile(fn, data, 0640); err != nil {
		return errors.Wrapf(err, "savePipeInfo(): could not write file %s ", fn)
	}

	return nil
}

func (sp *persister) onDeleteStream(name string) {
	fn := sp.pipeFileName(name)
	if err := os.Remove(fn); err != nil {
		sp.logger.Error("onDeleteStream(): could not delete file ", fn, ", for the name=", name, ", err=", err)
	}
}
