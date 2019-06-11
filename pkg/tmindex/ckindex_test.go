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
		idx, err := addData(cki, 0, 10, cnt)
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

	n := maxIntervalsPerBlock

	// level 2
	idx, _ := addData(cki, 0, 10, n*n+n/2)
	cnt, err := cki.count(idx)
	if cnt != n*n+n/2 || err != nil {
		t.Fatal(" expected cnt=", n*n+n/2, " but cnt=", cnt, ", err=", err)
	}
	if getUsedBlocks(cki) != n+4 {
		t.Fatal(" expected used at least, ", n+4, " but ", getUsedBlocks(cki))
	}

	// make it to level 1
	idx, _ = cki.addInterval(idx, interval{record{int64(2*n*10 + 3), uint32(2*n*10 + 3)}, record{int64(2*n*10 + 4), uint32(2*n*10 + 4)}})
	if getUsedBlocks(cki) != 3 {
		t.Fatal(" expected used at least, 3 but ", getUsedBlocks(cki))
	}

	// turn to level 0
	idx, _ = cki.addInterval(idx, interval{record{int64(2*n*10 - 23), uint32(2*n*10 - 23)}, record{int64(2*n*10 - 22), uint32(2*n*10 - 22)}})
	if getUsedBlocks(cki) != 1 {
		t.Fatal(" expected used at least 1 but ", getUsedBlocks(cki))
	}
	cnt, err = cki.count(idx)
	if cnt != int(n) || err != nil {
		t.Fatal("expecting cnt=", n, " but it is ", cnt, ", err=", err)
	}

	cki.deleteIndex(idx)
	if getUsedBlocks(cki) != 0 {
		t.Fatal(" expected 0 used, but ", getUsedBlocks(cki))
	}
}

func TestGeEqCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	idx, _ := addData(cki, 100, 1, 1100)
	r, err := cki.grEq(idx, 500)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 500 || r.ts != 500 {
		t.Fatal("wrong data ", r)
	}

	r, err = cki.grEq(idx, 50)
	if err != errAllMatches {
		t.Fatal("must be errAllMatches but err=", err)
	}

	r, err = cki.grEq(idx, 2600)
	if err != nil {
		t.Fatal("must be no errors, but last record r=", r, ", err=", err)
	}

	r, err = cki.grEq(idx, 1199)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 1198 || r.ts != 1198 {
		t.Fatal("wrong data ", r)
	}
}

func TestGetLessCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	idx, _ := addData(cki, 100, 1, 1100)
	r, err := cki.less(idx, 500)
	if err != nil {
		t.Fatal("must not be errors but err=", err)
	}

	if r.idx != 502 || r.ts != 502 {
		t.Fatal("wrong data ", r)
	}

	r, err = cki.less(idx, 5000)
	if err != errAllMatches {
		t.Fatal("must be errAllMatches but err=", err)
	}

	r, err = cki.less(idx, 90)
	if err != nil {
		t.Fatal("expecting err==nil, but err=", err)
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
		t.Fatal("WaitAllJobsDone was not blocked")
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

func TestCKITraversal(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	idx, err := addData(cki, 100, 1, 1100)
	if err != nil {
		t.Fatal(" unexpected err=", err)
	}
	res := make([]interval, 0, 10)
	res, err = cki.traversal(idx, res)
	if err != nil {
		t.Fatal("Something goes wrong err=", err)
	}

	if len(res) < 1100 {
		t.Fatal("expecting len 1100, but len(res)=", len(res))
	}

	ts := int64(102)
	for i := 2; i < len(res); i++ {
		it := res[i]
		if it.p0.ts != ts {
			t.Fatal(" at position i=", i, ", expected ts=", ts, ", but ", it)
		}
		ts += 2
	}

	idx, err = addDataExisting(cki, idx, 200, 1, 1)
	if err != nil {
		t.Fatal("unexpected err=", err)
	}

	res = make([]interval, 0, 10)
	res, err = cki.traversal(idx, res)
	if err != nil {
		t.Fatal("Something goes wrong err=", err)
	}

	if len(res) != 52 {
		t.Fatal("expecting len 52, but len(res)=", len(res))
	}

	ts = int64(102)
	for i := 2; i < len(res); i++ {
		it := res[i]
		if it.p0.ts != ts {
			t.Fatal(" at position i=", i, ", expected ts=", ts, ", but ", it)
		}
		ts += 2
	}
}

func TestInsertAndCorrectionCKI(t *testing.T) {
	cki := createNewCKI()
	defer cki.Close()

	root, _ := addData(cki, 10, 10, 1)    // {{10,10}, {20, 20}}
	addDataExisting(cki, root, 22, 8, 1)  // {{20,20}, {30, 30}}
	addDataExisting(cki, root, 30, 10, 1) // {{30,30}, {40, 40}}

	i0 := interval{record{10, uint32(10)}, record{20, uint32(20)}}
	i1 := interval{record{20, uint32(20)}, record{30, uint32(30)}}
	i2 := interval{record{30, uint32(30)}, record{40, uint32(40)}}
	ints, _ := cki.traversal(root, []interval{})
	if len(ints) != 3 || ints[0] != i0 || ints[1] != i1 || ints[2] != i2 {
		t.Fatal("wrong result ", ints)
	}

	addDataExisting(cki, root, 22, 4, 1) // {{22,22}, {26, 26}}
	i1 = interval{record{20, uint32(20)}, record{40, uint32(26)}}
	ints, _ = cki.traversal(root, []interval{})
	if len(ints) != 2 || ints[0] != i0 || ints[1] != i1 {
		t.Fatal("wrong result ", ints)
	}

	addDataExisting(cki, root, 12, 4, 1) // {{12,12}, {16, 16}}
	i0 = interval{record{10, uint32(10)}, record{40, uint32(16)}}
	ints, _ = cki.traversal(root, []interval{})
	if len(ints) != 1 || ints[0] != i0 {
		t.Fatal("wrong result ", ints)
	}

	addDataExisting(cki, root, 10, 4, 1) // {{10,10}, {14, 14}}
	i0 = interval{record{10, uint32(10)}, record{40, uint32(14)}}
	ints, _ = cki.traversal(root, []interval{})
	if len(ints) != 1 || ints[0] != i0 {
		t.Fatal("wrong result ", ints)
	}

	addDataExisting(cki, root, 2, 40, 1) // {{2,2}, {42, 42}}
	i0 = interval{record{2, uint32(2)}, record{42, uint32(42)}}
	ints, _ = cki.traversal(root, []interval{})
	if len(ints) != 1 || ints[0] != i0 {
		t.Fatal("wrong result ", ints)
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

func addData(cki *ckindex, start, step, count int) (int, error) {
	idx, err := cki.addInterval(-1, interval{record{int64(start), uint32(start)}, record{int64(start + step), uint32(start + step)}})
	if err != nil {
		return idx, err
	}

	return addDataExisting(cki, idx, start+step, step, count-1)
}

func addDataExisting(cki *ckindex, idx int, start, step, count int) (int, error) {
	for ts := 0; ts < count; ts++ {
		idx1, err := cki.addInterval(idx, interval{record{int64(2*ts*step + start), uint32(2*ts*step + start)}, record{int64(2*ts*step + start + step), uint32(2*ts*step + start + step)}})
		if err != nil {
			return idx, err
		}
		idx = idx1
	}
	return idx, nil
}
