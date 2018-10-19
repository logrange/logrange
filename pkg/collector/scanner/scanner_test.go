package scanner

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/logrange/logrange/pkg/collector/model"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jrivets/log4g"
)

const (
	testOutDir = "/tmp/scannertest"
)

var (
	inOutMap = map[string]string{}
	outFiles = map[string]*os.File{}
	log      = log4g.GetLogger("TestIntegration")
)

func TestIntegration(t *testing.T) {
	os.RemoveAll(testOutDir)
	os.MkdirAll(testOutDir, 0777)

	lg := log4g.GetLogger("collector.scanner")
	log4g.SetLogLevel(lg.GetName(), log4g.DEBUG)

	cfg := NewDefaultConfig()
	cfg.ScanPathsIntervalSec = 5

	_, d, _, _ := runtime.Caller(0)
	cfg.ScanPaths = []string{
		filepath.Dir(d) + "/testdata/logs/ubuntu/var/log/*.log",
		filepath.Dir(d) + "/testdata/logs/ubuntu/var/log/*/*.log",
	}

	cfg.FileFormats = append(cfg.FileFormats, &FileFormat{
		PathMatcher: ".*",
		DataFormat:  cTxtDataFormat,
		TimeFormats: []string{"DD/MMM/YYYY:HH:mm:ss ZZZZ"},
	})

	cl, err := NewScanner(cfg, NewInMemStateStorage())
	if err != nil {
		t.Fatal(err)
	}
	err = cl.Start()
	if err != nil {
		t.Fatal(err)
	}

	for {
		select {
		case ev := <-cl.Events():
			if err := handle(ev); err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second * 5):
			if err := cl.Stop(); err != nil {
				t.Error(err)
			}

			closeOutFiles()
			if len(inOutMap) == 0 {
				log.Error("nothing was sent, nothing to compare!")
				t.FailNow()
			}
			for src, dst := range inOutMap {
				log.Info("comparing src=", src, ", dst=", dst)
				eq, err := md5Eq(src, dst)
				if err != nil || !eq {
					log.Error("failed to compare src=", src, ", dst=", dst, "; err=", err, ", eq=", eq)
					t.Fail()
				}
			}
			return
		}
	}
}

func handle(ev *model.Event) error {
	tFile := fmt.Sprintf("%v/%v", testOutDir, path.Base(ev.File))
	for _, r := range ev.Records {
		if err := appendToFile(tFile, r); err != nil {
			return err
		}
	}

	ev.Confirm()
	inOutMap[ev.File] = tFile
	return nil
}

func appendToFile(file string, r *model.Record) error {
	fd, err := getOutFile(file)
	if err != nil {
		return err
	}

	/*	bb := bytes.Buffer{}
		bb.Write([]byte{'\n', '>', 't', 's', '='})
		bb.Write([]byte((*r.GetTs()).Format(time.RFC3339Nano)))
		bb.Write([]byte{'|'})
		bb.Write(r.Data)
		bb.Write([]byte{'<'})*/
	_, err = fd.Write(r.Data)
	return err
}

func getOutFile(file string) (*os.File, error) {
	if f, ok := outFiles[file]; ok {
		return f, nil
	}

	fd, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}

	outFiles[file] = fd
	return fd, nil
}

func closeOutFiles() {
	for _, fd := range outFiles {
		fd.Sync()
		fd.Close()
	}
}

func md5Eq(f1, f2 string) (bool, error) {
	f1h, err := md5File(f1)
	log.Info("file ", f1, " hash=", f1h)
	if err != nil {
		return false, err
	}

	f2h, err := md5File(f2)
	log.Info("file ", f2, " hash=", f2h)
	if err != nil {
		return false, err
	}

	return bytes.Equal(f1h, f2h), nil
}

func md5File(file string) ([]byte, error) {
	fi, _ := os.Stat(file)
	log.Info("file=", file, " size=", fi.Size())

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, f); err != nil {
		return nil, err
	}

	var result []byte
	return hash.Sum(result), err
}
