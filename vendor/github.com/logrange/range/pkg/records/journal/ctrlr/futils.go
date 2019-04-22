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

package ctrlr

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/chunk/chunkfs"
	"github.com/logrange/range/pkg/utils/fileutil"
)

// scanForJournals receives a dir and scans it for journaling databse strucutre.
// It returns list of journal names (not folders!!!) found in the dir structure
//
// The journaling database is stored in the following structure of subfolders
// which reside under the dir: <jgroup>/<ejname>/<jchunks>
// jgroup - is a directory which name is last 2 ASCII characters of ejname(s)
// 			stored in there. All ejnames with names, which suffix is this 2 chars are
// 			placed in the folder
// ejname - escaped journal name. All journal names are escaped for beint FS naming
// 			convenience friendly
// chunks - a set of chunks for the journal. Files are here
func scanForJournals(dir string) ([]string, error) {
	err := fileutil.EnsureDirExists(dir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, 10)
	err = filepath.Walk(dir, func(pth string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			return nil
		}

		nm := info.Name()
		if len(nm) != 2 {
			return nil
		}

		return filepath.Walk(pth, func(pth2 string, info2 os.FileInfo, err error) error {
			if !info2.IsDir() || pth == pth2 {
				return nil
			}

			ejn := info2.Name()
			if strings.HasSuffix(ejn, nm) {
				res = append(res, fileutil.UnescapeFileName(ejn))
			}
			return nil
		})
	})
	return res, err
}

// scanForChunks scans a journal directory to find out data chunks there
func scanForChunks(dir string, removeEmpty bool) ([]chunk.Id, error) {
	res := make([]chunk.Id, 0, 3)
	toRemove := make([]string, 0, 3)
	err := filepath.Walk(dir, func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		_, file := filepath.Split(pth)
		if filepath.Ext(file) != chunkfs.ChnkDataExt {
			return nil
		}
		name := fileutil.SetFileExt(file, "")

		cid, err := chunk.ParseId(name)
		if err != nil {
			return nil
		}

		if info.Size() == 0 && removeEmpty {
			toRemove = append(toRemove, pth)
			return nil
		}

		res = append(res, cid)
		return nil
	})

	for _, cfn := range toRemove {
		fn := chunkfs.SetChunkDataFileExt(cfn)
		os.Remove(fn)
		fn = chunkfs.SetChunkIdxFileExt(cfn)
		os.Remove(fn)
	}

	return res, err
}

func deleteChunkFilesIfEmpty(name string) bool {
	res := false
	fn := chunkfs.SetChunkDataFileExt(name)
	if isFileEmpty(fn) {
		res = true
		os.Remove(fn)
	}

	fn = chunkfs.SetChunkIdxFileExt(name)
	if res || isFileEmpty(fn) {
		os.Remove(fn)
	}
	return res
}

func isFileEmpty(fn string) bool {
	fi, err := os.Stat(fn)
	return err == nil && fi.Size() == 0
}

func checkPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func journalPath(baseDir, jname string) (string, error) {
	ejn := fileutil.EscapeToFileName(jname)
	if len(ejn) < 2 {
		return "", fmt.Errorf("Journal folder name must be at least 2 chars length. ejname=%s from the original name=%s", ejn, jname)
	}
	return filepath.Join(baseDir, ejn[len(ejn)-2:], ejn), nil
}
