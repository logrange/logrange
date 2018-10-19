package fs

import (
	"github.com/logrange/logrange/pkg/records"
)

type (
	ChunkReader struct {
	}
)

func (cr *ChunkReader) Next() {

}

func (cr *ChunkReader) Get() (records.Iterator, error) {
	return nil, nil
}

func (cr *ChunkReader) Seek(pos uint64) error {
	return nil
}

func (cr *ChunkReader) SetForwardDirection(forward bool) {

}

func (cr *ChunkReader) Close() error {
	return nil
}
