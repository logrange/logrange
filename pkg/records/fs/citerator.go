package fs

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/logrange/logrange/pkg/records"
)

type (
	cIterator struct {
		cid    uint64
		fdPool *FdPool
		cr     cReader
		size   *int64
		pos    int64
		nPos   int64
		buf    []byte
		res    []byte
		rdFunc func(buf []byte) ([]byte, int64, error)

		// bkwd the iterator direction
		bkwd bool
	}
)

func newCIterator(cid uint64, fdPool *FdPool, lro, size *int64, buf []byte) *cIterator {
	ci := new(cIterator)
	ci.cid = cid
	ci.fdPool = fdPool
	ci.cr.lro = lro
	ci.size = size
	ci.buf = buf[:cap(buf)]
	ci.rdFunc = ci.cr.readRecord
	return ci
}

func (ci *cIterator) Close() error {
	ci.Release()
	ci.fdPool = nil
	ci.buf = nil
	return nil
}

func (ci *cIterator) SetBackward(bkwd bool) {
	if ci.bkwd == bkwd {
		return
	}

	ci.bkwd = bkwd
	if ci.bkwd {
		ci.rdFunc = ci.cr.readRecordBack
	} else {
		ci.rdFunc = ci.cr.readRecord
	}

	pos := ci.pos
	ci.pos-- // to be sure SetPos will make the change
	ci.SetPos(pos)

}

func (ci *cIterator) Next(ctx context.Context) {
	_, err := ci.Get(ctx)
	if err == nil {
		ci.pos = ci.nPos
	}

	ci.res = nil
}

func (ci *cIterator) Get(ctx context.Context) (records.Record, error) {
	if ci.res != nil {
		return ci.res, nil
	}

	err := ci.ensureFileReader(ctx)
	if err != nil {
		return nil, err
	}

	ci.res, ci.nPos, err = ci.rdFunc(ci.buf)
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

func (ci *cIterator) Pos() int64 {
	return ci.pos
}

func (ci *cIterator) SetPos(pos int64) int64 {
	if ci.pos != pos {
		size := atomic.LoadInt64(ci.size)
		if pos >= size {
			if ci.bkwd {
				pos = atomic.LoadInt64(ci.cr.lro)
			} else {
				pos = size
			}
		}
		if pos < 0 {
			if ci.bkwd {
				pos = -1
			} else {
				pos = 0
			}
		}
		ci.pos = pos
		ci.res = nil
	}
	return ci.pos
}

func (ci *cIterator) ensureFileReader(ctx context.Context) error {
	if ci.pos < 0 || ci.pos >= atomic.LoadInt64(ci.size) {
		return io.EOF
	}

	var err error
	if ci.cr.fr == nil {
		ci.cr.fr, err = ci.fdPool.acquire(ctx, ci.cid, ci.pos)
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
