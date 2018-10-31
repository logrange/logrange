package journal

import (
	"context"
	"io"

	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"
)

type (
	jIterator struct {
		j   *journal
		pos JPos
		ci  records.ChunkIterator
	}
)

func (ji *jIterator) Next() {
	ji.Get()
	if ji.ci != nil {
		ji.ci.Next()
	}
}

func (ji *jIterator) Get() (records.Record, error) {
	return ji.GetCtx(context.Background())
}

func (ji *jIterator) GetCtx(ctx context.Context) (records.Record, error) {
	err := ji.ensureChkIt()
	if err != nil {
		return nil, err
	}

	rec, err := ji.ci.GetCtx(ctx)
	if err == io.EOF {
		ji.Release()
		ji.pos.cid++

		err = ji.ensureChkIt()
		if err != nil {
			return nil, err
		}
		return ji.ci.GetCtx(ctx)

	}
	return rec, err
}

func (ji *jIterator) Pos() JPos {
	return ji.pos
}

func (ji *jIterator) SetPos(pos JPos) {
	if pos == ji.pos {
		return
	}

	ji.Release()
	ji.pos = pos
	ji.correctPos()
}

func (ji *jIterator) Release() {
	if ji.ci != nil {
		ji.ci.Close()
		ji.ci = nil
	}
}

func (ji *jIterator) Close() error {
	ji.Release()
	ji.j = nil
	return nil
}

func (ji *jIterator) correctPos() records.Chunk {
	chk := ji.j.getChunkById(ji.pos.cid)
	if chk == nil {
		return chk
	}
	if chk.Id() < ji.pos.cid {
		ji.pos.cid = chk.Id()
		ji.pos.roffs = chk.Size()
	} else if chk.Id() > ji.pos.cid {
		ji.pos.cid = chk.Id()
		ji.pos.roffs = 0
	}
	return chk
}

func (ji *jIterator) ensureChkIt() error {
	if ji.ci != nil {
		return nil
	}

	chk := ji.correctPos()
	if chk == nil {
		return util.ErrWrongState
	}

	var err error
	ji.ci, err = chk.Iterator()
	return err
}
