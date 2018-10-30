package fs

import (
	"context"

	"github.com/logrange/logrange/pkg/records"
)

type (
	cIterator struct {
		fileName string
		fdPool   *FdPool
		cr       cReader
		pos      int64
		buf      []byte
		res      []byte
	}
)

func newCIterator(fileName string, fdPool *FdPool, lro *int64, buf []byte) *cIterator {
	ci := new(cIterator)
	ci.fileName = fileName
	ci.fdPool = fdPool
	ci.cr.lro = lro
	ci.buf = buf[:cap(buf)]
	return ci
}

func (ci *cIterator) Close() error {
	ci.Release()
	ci.fdPool = nil
	ci.buf = nil
	return nil
}

func (ci *cIterator) Next() {
	_, err := ci.Get()
	if err == nil {
		ci.pos = ci.cr.getPos()
	}

	ci.res = nil
}

func (ci *cIterator) Get() (records.Record, error) {
	return ci.GetCtx(context.Background())
}

func (ci *cIterator) GetCtx(ctx context.Context) (records.Record, error) {
	if ci.res != nil {
		return ci.res, nil
	}

	err := ci.ensureFileReader(ctx)
	if err != nil {
		return nil, err
	}

	ci.res, err = ci.cr.readRecord(ci.buf)
	if err != nil {
		ci.Release()
	}

	return ci.res, err
}

func (ci *cIterator) Release() {
	if ci.cr.fr != nil {
		ci.fdPool.release(ci.cr.fr)
		ci.cr.fr = nil
	}
	ci.res = nil
}

func (ci *cIterator) GetPos() int64 {
	return ci.pos
}

func (ci *cIterator) SetPos(pos int64) {
	if ci.pos != pos {
		ci.pos = pos
		ci.res = nil
	}
}

func (ci *cIterator) ensureFileReader(ctx context.Context) error {
	var err error
	if ci.cr.fr == nil {
		ci.cr.fr, err = ci.fdPool.acquire(ctx, ci.fileName, ci.pos)
		if err != nil {
			return err
		}
	}

	if ci.cr.getPos() != ci.pos {
		err := ci.cr.fr.seek(ci.pos)
		if err != nil {
			ci.Release()
			return err
		}
	}
	return nil
}
