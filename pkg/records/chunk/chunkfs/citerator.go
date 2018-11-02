package chunkfs

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/logrange/logrange/pkg/records"
)

type (
	cIterator struct {
		gid    uint64
		fdPool *FdPool
		cr     cReader
		cnt    *uint32
		pos    uint32
		buf    []byte
		res    []byte

		// cached values to acquire readers
		doffs int64
	}
)

func newCIterator(gid uint64, fdPool *FdPool, count *uint32, buf []byte) *cIterator {
	ci := new(cIterator)
	ci.gid = gid
	ci.fdPool = fdPool
	ci.cnt = count
	ci.pos = 0
	ci.buf = buf[:cap(buf)]
	return ci
}

func (ci *cIterator) Close() error {
	ci.Release()
	ci.fdPool = nil
	ci.buf = nil
	return nil
}

func (ci *cIterator) Next(ctx context.Context) {
	_, err := ci.Get(ctx)
	if err == nil {
		ci.pos++
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

	ci.res, err = ci.cr.readRecord(ci.buf)
	if err != nil {
		ci.Release()
	}

	return ci.res, err
}

func (ci *cIterator) Release() {
	if ci.cr.dr != nil {
		ci.doffs = ci.cr.dr.getNextReadPos()
		ci.fdPool.release(ci.cr.dr)
		ci.cr.dr = nil
	}

	if ci.cr.ir != nil {
		ci.fdPool.release(ci.cr.ir)
		ci.cr.ir = nil
	}
	ci.res = nil
}

func (ci *cIterator) Pos() uint32 {
	return ci.pos
}

func (ci *cIterator) SetPos(pos uint32) error {
	if pos == ci.pos {
		return nil
	}

	if pos > atomic.LoadUint32(ci.cnt) {
		return ErrWrongOffset
	}

	if ci.pos < pos {
		ci.doffs = 0
	}
	ci.pos = pos
	return ci.cr.setPos(ci.pos)
}

func (ci *cIterator) ensureFileReader(ctx context.Context) error {
	if ci.pos < 0 || ci.pos >= atomic.LoadUint32(ci.cnt) {
		return io.EOF
	}

	var err error
	if ci.cr.dr == nil {
		ci.cr.dr, err = ci.fdPool.acquire(ctx, ci.gid, ci.doffs)

		if ci.cr.ir == nil && err == nil {
			ci.cr.ir, err = ci.fdPool.acquire(ctx, ci.gid+1, int64(ci.pos)*int64(ChnkIndexRecSize))
		}

		if err == nil {
			err = ci.cr.setPos(ci.pos)
		} else {
			ci.Release()
		}
	}

	return err
}
