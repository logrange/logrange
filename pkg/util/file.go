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

package util

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// GetFileId generates an id by file name and its info. The id can help to identify
// whether the file content was rewritten or not. For example, if two identifiers
// calculated for same file name are different, we assume the file content was
// rewritten between first and the second identifiers calculations. If the
// identifiers are same, we assume that new data could be added to the file,
// but previously written one stays unchanged.
func GetFileId(file string, info os.FileInfo) string {
	stat := info.Sys().(*syscall.Stat_t)
	return fmt.Sprintf("%v_%v_%v", Md5(file), stat.Ino, stat.Dev)
}

// ExpandPaths walks through provided paths and turn them to list of files.
// The input paths can, for instance, contain ["/var/log/*.log"], so the
// method will return list of files from the /var/log/ folder, which have
// .log extension.
func ExpandPaths(paths []string) []string {
	result := make([]string, 0, len(paths))
	for _, pp := range paths {
		gg, err := filepath.Glob(pp)
		if err != nil {
			continue
		}
		for _, g := range gg {
			result = append(result, g)
		}
	}
	return result
}

// SetFileExt changes file extension to ext. ext can be empty, then the result
// will have no extension
func SetFileExt(file, ext string) string {
	if len(ext) > 0 && ext[0] != '.' {
		ext = "." + ext
	}
	e := filepath.Ext(file)
	return file[:len(file)-len(e)] + ext
}

// EscapeToFileName receives a name and turns it to a file-system file name.
// it escapes (substitute) slashes '/' at least.
func EscapeToFileName(name string) string {
	// TODO
	return name
}

// UnescapeFileName receives a file name and un-escape it. It supposes that the
// file name was escaped by EscapeToFileName() before
func UnescapeFileName(fname string) string {
	//TODO
	return fname
}
