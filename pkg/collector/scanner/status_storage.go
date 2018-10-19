package scanner

import (
	"github.com/logrange/logrange/pkg/util"
	"io/ioutil"
	"os"
)

type (
	// StateStorage interface allows to read and write serialized data of scanner status
	StateStorage interface {
		// ReadData will return data buffer. If the status is not
		// found os.IsNotExist(err) will be true.
		ReadData() ([]byte, error)
		WriteData(buf []byte) error
	}

	// inmemStateStorage struct an in-mem StateStorage implementation
	inmemStateStorage struct {
		buf []byte
	}

	// fileStateStorage stuct a file StateStorage implementation
	fileStateStorage struct {
		fileName string
	}
)

func NewInMemStateStorage() StateStorage {
	return &inmemStateStorage{}
}

func NewFileStateStorage(fn string) StateStorage {
	return &fileStateStorage{fn}
}

func (iss *inmemStateStorage) ReadData() ([]byte, error) {
	if iss.buf == nil {
		return nil, os.ErrNotExist
	}
	return iss.buf, nil
}

func (iss *inmemStateStorage) WriteData(buf []byte) error {
	if buf == nil {
		iss.buf = nil
		return nil
	}
	iss.buf = util.BytesCopy(buf)
	return nil
}

func (fss *fileStateStorage) ReadData() ([]byte, error) {
	return ioutil.ReadFile(fss.fileName)
}

func (fss *fileStateStorage) WriteData(buf []byte) error {
	return ioutil.WriteFile(fss.fileName, buf, 0640)
}
