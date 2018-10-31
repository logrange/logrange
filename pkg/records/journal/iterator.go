package journal

import (
	"context"
	"io"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	iterator struct {
		j   *journal
		pos Pos
		//c   records.Chunk
		ci chunk.Iterator
		// the bkwd flag indicates that the direction is backward
		bkwd bool
	}
)

func (it *iterator) Next(ctx context.Context) {
	it.Get(ctx)
	if it.ci != nil {
		it.ci.Next(ctx)
		it.pos.Offs = it.ci.Pos()
	}
}

func (it *iterator) Get(ctx context.Context) (records.Record, error) {
	err := it.ensureChkIt(it.pos.CId)
	if err != nil {
		return nil, err
	}

	rec, err := it.ci.Get(ctx)
	if err == io.EOF {
		err = it.advanceChunk()
		if err != nil {
			return nil, err
		}
		rec, err = it.ci.Get(ctx)
	}
	return rec, err
}

func (it *iterator) Pos() Pos {
	return it.pos
}

func (it *iterator) Reset(pos Pos, bkwd bool) {
	if pos == it.pos && it.bkwd == bkwd {
		return
	}

	it.bkwd = bkwd

	if pos.CId != it.pos.CId {
		it.Release()
	}

	if it.ci != nil {
		it.ci.SetBackward(bkwd)
		it.ci.SetPos(pos.Offs)
	}
	it.pos = pos
}

func (it *iterator) Close() error {
	it.Release()
	it.j = nil
	return nil
}

func (it *iterator) Release() {
	if it.ci != nil {
		it.ci.Close()
		it.ci = nil
	}
}

func (it *iterator) advanceChunk() error {
	it.Release()

	cid := it.pos.CId
	if it.bkwd {
		cid--
	} else {
		cid++
	}

	return it.ensureChkIt(cid)
}

// findChunkByPos looks for the chunk and updates the pos if needed
func (it *iterator) findChkAndCorrectPos(cid uint64) chunk.Chunk {
	chk := it.j.getChunkById(cid, it.bkwd)
	if chk == nil {
		return nil
	}

	it.pos.CId = chk.Id()
	if chk.Id() < cid {
		it.pos.Offs = chk.Size()
	} else if chk.Id() > cid {
		it.pos.Offs = -1
	}

	return chk
}

func (it *iterator) ensureChkIt(cid uint64) error {
	if it.ci != nil {
		return nil
	}

	chk := it.findChkAndCorrectPos(cid)
	if chk == nil {
		return io.EOF
	}

	var err error
	it.ci, err = chk.Iterator()
	if err != nil {
		return err
	}
	it.pos.Offs = it.ci.SetPos(it.pos.Offs)
	return nil
}
