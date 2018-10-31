package jrnlfs

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	ChnkDataExt = ".dat"
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
	err := ensureDirExists(dir)
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
				res = append(res, eJName2JName(ejn))
			}
			return nil
		})
	})
	return res, err
}

// eJName2JName gets escaped journal name (ejn), which is actually a sub-directory
// name, and returns its journal name. Unescaping it.
func eJName2JName(ejn string) string {
	//TODO: implement me properly
	return ejn
}

// jName2eJName gets the journal name and turns it to FS dir name.
func jName2eJName(jn string) string {
	// TODO: implement me properly
	return jn
}

// scanForChunks scans a journal directory to find out data chunks there
func scanForChunks(dir string) ([]uint64, error) {
	res := []uint64{}
	err := filepath.Walk(dir, func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		_, file := filepath.Split(pth)
		if filepath.Ext(file) != ChnkDataExt {
			return nil
		}
		name := file[:len(file)-len(ChnkDataExt)]

		id, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			return nil
		}
		res = append(res, uint64(id))
		return nil
	})
	return res, err
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

func journalPath(baseDir, jid string) (string, error) {
	if len(jid) < 2 {
		return "", errors.New("Journal Id must be at least 2 chars len" + jid)
	}
	jpath := filepath.Join(baseDir, jid[len(jid)-2:], jid)
	err := ensureDirExists(jpath)
	return jpath, err
}

func ensureDirExists(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0740)
		}
	} else {
		d.Close()
	}
	return err
}
