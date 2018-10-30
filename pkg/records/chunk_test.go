package records

import (
	"testing"
	"time"
)

func TestNewCid(t *testing.T) {
	lastCid = 0
	cid := NewChunkId()
	cid2 := NewChunkId()
	diff := (cid2 - cid) >> 16
	if cid == cid2 || cid2 < cid || diff > 1 {
		t.Fatal("Ooops something wrong with cid generation cid=", cid, " cid2=", cid2, ", diff=", diff)
	}

	// some diff
	cid = NewChunkId()
	time.Sleep(time.Millisecond)
	cid2 = NewChunkId()
	diff = (cid2 - cid) >> 16
	if cid == cid2 || cid2 < cid || diff > 10000 || cid2 != lastCid {
		t.Fatal("Ooops something wrong with cid generation cid=", cid, " cid2=", cid2, ", diff=", diff)
	}

	// now put far to the future
	lastCid += uint64(time.Hour)
	lcid := lastCid
	cid = NewChunkId()
	cid2 = NewChunkId()
	diff = (cid - lcid) >> 16
	diff2 := (cid2 - lcid) >> 16
	if diff != 1 || diff2 != 2 {
		t.Fatal("expecting diff1=1 and diff2=2, but got ", diff, " and ", diff2, " respectively")
	}
}
