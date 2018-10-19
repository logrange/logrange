package scanner

import (
	"context"
	"github.com/logrange/logrange/pkg/util"
	"os"
	"path/filepath"
	"runtime"

	"github.com/jrivets/log4g"
	"testing"
)

func TestStringParser(t *testing.T) {
	logger := log4g.GetLogger("TestStringParser")
	_, d, _, _ := runtime.Caller(0)
	fTmps := []string{
		filepath.Dir(d) + "/testdata/logs/ubuntu/var/log/wifi*.log",
		filepath.Dir(d)+ "/testdata/logs/ubuntu/var/log/*/*.log",
	}

	files := getFilePaths(fTmps)
	logger.Info("files=", files)

	for _, fn := range files {
		f, err := os.Open(fn)
		if err != nil {
			logger.Warn("Could not open file ", fn, ", err=", err)
		}
		lr := newLineReader(f, 64000, context.Background())
		lp := newStringParser()
		for err == nil {
			var ln []byte
			ln, err = lr.readLine()
			if err == nil {
				lp.apply(ln)
			}
		}
		f.Close()
		logger.Info("File read stats: \nfilename=", fn, lp.stats)
	}
}

func getFilePaths(paths []string) []string {
	ep := util.ExpandPaths(paths)
	ff := make([]string, 0, len(ep))
	for _, p := range ep {
		if _, err := filepath.EvalSymlinks(p); err != nil {
			continue
		}
		fin, err := os.Stat(p)
		if err != nil {
			continue
		}
		if fin.IsDir() {
			continue
		}
		ff = append(ff, p)
	}
	return util.RemoveDups(ff)
}
