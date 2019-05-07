// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tmindex

import (
	"fmt"
	"github.com/logrange/range/pkg/bstorage"
	"testing"
	"time"
)

func TestAddRecordsAndCountToCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	for cnt := 10; cnt < 3000; cnt += 50 {
		idx, err := addData(cki, 0, cnt)
		if err != nil {
			t.Fatal("Must be able to be added, but err=", err)
		}

		count, err := cki.count(idx)
		if err != nil || count != cnt {
			t.Fatal("expected count=", cnt, ", and no error, but count=", count, ", err=", err)
		}

		err = cki.deleteIndex(idx)
		if err != nil {
			t.Fatal("could not delete index with idx=", idx, " err=", err)
		}

		if cki.bks.Available() != cki.bks.Count() {
			t.Fatal("availabe ", cki.bks.Available(), ", but total count is ", cki.bks.Count(), ", or an err=", err)
		}
	}
}

func TestPruneCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	// level 3
	idx, _ := addData(cki, 0, maxRecsPerBlock*maxRecsPerBlock+20)
	if getUsedBlocks(cki) <= maxRecsPerBlock {
		t.Fatal(" expected used at least, ", maxRecsPerBlock, " but ", getUsedBlocks(cki))
	}

	idx, _ = cki.addRecord(idx, record{maxRecsPerBlock + 1, maxRecsPerBlock + 1})
	if getUsedBlocks(cki) <= 2 {
		t.Fatal(" expected used at least, 2 but ", getUsedBlocks(cki))
	}

	idx, _ = cki.addRecord(idx, record{maxRecsPerBlock - 1, maxRecsPerBlock - 1})
	if getUsedBlocks(cki) != 1 {
		t.Fatal(" expected used at least 1 but ", getUsedBlocks(cki))
	}

	idx, _ = cki.addRecord(idx, record{maxRecsPerBlock, maxRecsPerBlock})
	if getUsedBlocks(cki) != 3 {
		t.Fatal(" expected used at least 3 but ", getUsedBlocks(cki))
	}

	cki.deleteIndex(idx)
	if getUsedBlocks(cki) != 0 {
		t.Fatal(" expected 0 used, but ", getUsedBlocks(cki))
	}
}

func TestGeEqCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	idx, _ := addData(cki, 100, 1100)
	r, err := cki.grEq(idx, 500)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 500 || r.ts != 500 {
		t.Fatal("wrong data ", r)
	}

	r, err = cki.grEq(idx, 50)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 100 || r.ts != 100 {
		t.Fatal("wrong data ", r)
	}

	r, err = cki.grEq(idx, 1300)
	if err != nil || r.idx != 1199 || r.ts != 1199 {
		t.Fatal("must not be error, but r=", r, ", err=", err)
	}

	r, err = cki.grEq(idx, 1199)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 1199 || r.ts != 1199 {
		t.Fatal("wrong data ", r)
	}
}

func TestGetLessCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	idx, _ := addData(cki, 100, 1100)
	r, err := cki.less(idx, 500)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 510 || r.ts != 510 {
		t.Fatal("wrong data ", r)
	}

	r, err = cki.less(idx, 5000)
	if err != errAllMatches {
		t.Fatal("must not be errors but err=", err)
	}

	r, err = cki.less(idx, 100)
	if r.idx != 100 || r.ts != 100 {
		t.Fatal("wrong data ", r)
	}

}

func TestCKICLoseBlocked(t *testing.T) {
	cki := createNewCKI()
	cki.acquireRead()
	tm := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		cki.releaseRead()
	}()

	cki.Close()
	if time.Now().Sub(tm) < 5*time.Millisecond {
		t.Fatal("Close was not blocked")
	}
}

func TestCKIExecExlusively(t *testing.T) {
	cki := createNewCKI()
	cki.acquireRead()
	tm := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		cki.releaseRead()
	}()

	var t2, t3 time.Time
	cki.execExclusively(func() {
		t2 = time.Now()
		go cki.execExclusively(func() {
			t3 = time.Now()
			time.Sleep(5 * time.Millisecond)
		})
		time.Sleep(5 * time.Millisecond)
	})
	cki.acquireRead()
	t4 := time.Now()
	cki.releaseRead()

	if t2.Sub(tm) < 5*time.Millisecond {
		t.Fatal("t2=", t2, " close to tm", tm)
	}

	if t3.Sub(t2) < 5*time.Millisecond {
		t.Fatal("t3=", t3, " close to t2", t2)
	}

	if t4.Sub(t3) < time.Millisecond {
		t.Fatal("t4=", t4, " close to t3", t3)
	}
}

func createNewCKI() *ckindex {
	bks := bstorage.GetBlocksInSegment(blockSize)
	bts := bstorage.NewInMemBytes(2 * bks * blockSize)
	fmt.Println("Will create ", 2*bks, " blocks with total size=", bts)
	bs, err := bstorage.NewBlocks(blockSize, bts, true)
	if err != nil {
		panic(err)
	}
	return newCkIndex(bs)
}

func getUsedBlocks(cki *ckindex) int {
	return cki.bks.Count() - cki.bks.Available()
}

func addData(cki *ckindex, start, count int) (int, error) {
	idx, err := cki.addRecord(-1, record{int64(start), uint32(start)})
	if err != nil {
		return idx, err
	}

	for ts := 1; ts < count; ts++ {
		idx1, err := cki.addRecord(idx, record{int64(ts + start), uint32(ts + start)})
		if err != nil {
			return idx, err
		}
		idx = idx1
	}
	return idx, nil
}
