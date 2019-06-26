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
	"encoding/json"
	"github.com/logrange/range/pkg/records/chunk"
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	v := map[string]sortedChunks{"aaa": {&chkInfo{Id: chunk.NewId(), MinTs: 1234, MaxTs: 1234, IdxRoot: Item{1234, 123}}}}
	res, err := json.Marshal(v)
	if err != nil {
		t.Fatal("could not marshal v=", v, ", err=", err)
	}

	var v1 map[string]sortedChunks
	err = json.Unmarshal(res, &v1)
	if err != nil {
		t.Fatal("could not unmarshal res=", string(res), ", err=", err)
	}

	if !reflect.DeepEqual(v, v1) {
		t.Fatalf("v=%v, v1=%v", v, v1)
	}
}

func TestSortedChunkApply(t *testing.T) {
	testSortedChunkApply(t,
		sortedChunks{&chkInfo{Id: 1, MinTs: 0, MaxTs: 1}},
		sortedChunks{&chkInfo{Id: 1, MinTs: 0, MaxTs: 2}},
		sortedChunks{&chkInfo{Id: 1, MinTs: 0, MaxTs: 2}},
		sortedChunks{},
	)
	testSortedChunkApply(t,
		sortedChunks{},
		sortedChunks{&chkInfo{Id: 1, MinTs: 0, MaxTs: 2}},
		sortedChunks{},
		sortedChunks{&chkInfo{Id: 1, MinTs: 0, MaxTs: 2}},
	)

	testSortedChunkApply(t,
		sortedChunks{&chkInfo{Id: 1, MinTs: 0, MaxTs: 2}},
		sortedChunks{&chkInfo{Id: 1, MinTs: 20, MaxTs: 22}, &chkInfo{Id: 2, MinTs: 0, MaxTs: 2}},
		sortedChunks{&chkInfo{Id: 1, MinTs: 20, MaxTs: 22}},
		sortedChunks{&chkInfo{Id: 2, MinTs: 0, MaxTs: 2}},
	)

	testSortedChunkApply(t,
		sortedChunks{&chkInfo{Id: 10, MinTs: 0, MaxTs: 2}},
		sortedChunks{&chkInfo{Id: 2, MinTs: 20, MaxTs: 22}, &chkInfo{Id: 10, MinTs: 10, MaxTs: 12}},
		sortedChunks{&chkInfo{Id: 10, MinTs: 10, MaxTs: 12}},
		sortedChunks{&chkInfo{Id: 2, MinTs: 20, MaxTs: 22}},
	)

	testSortedChunkApply(t,
		sortedChunks{&chkInfo{Id: 10, MinTs: 0, MaxTs: 2}},
		sortedChunks{&chkInfo{Id: 2, MinTs: 20, MaxTs: 22}, &chkInfo{Id: 3, MinTs: 10, MaxTs: 12}},
		sortedChunks{&chkInfo{Id: 10, MinTs: 0, MaxTs: 2}},
		sortedChunks{&chkInfo{Id: 2, MinTs: 20, MaxTs: 22}, &chkInfo{Id: 3, MinTs: 10, MaxTs: 12}},
	)
}

func testSortedChunkApply(t *testing.T, sc1, sc2, res, rmvd sortedChunks) {
	testSortedChunkApply1(t, false, sc1, sc2, res, rmvd)
	testSortedChunkApply1(t, true, sc1, sc2, res, rmvd)
}

func testSortedChunkApply1(t *testing.T, copyData bool, sc1, sc2, res, rmvd sortedChunks) {
	res1, rmvd1 := sc1.apply(sc2, copyData)
	if len(res1) != len(res) || len(rmvd1) != len(rmvd) {
		t.Fatal("res=", res, " res1=", res1, " rmvd=", rmvd, " rmvd1=", rmvd1)
	}

	for i, r := range res1 {
		if !copyData {
			if *r != *res[i] {
				t.Fatal("copyData==false expecting ", res[i], " but ", r, " at index ", i, " of ", res1)
			}
		} else {
			if r.Id != res[i].Id || r.MinTs != res[i].MinTs || r.MaxTs != res[i].MaxTs {
				t.Fatal("copyData==true expecting ", res[i], " but ", r, " at index ", i, " of ", res1)
			}
		}
	}

	for i, r := range rmvd1 {
		if *r != *rmvd[i] {
			t.Fatal("expecting ", rmvd[i], " but ", r, " at index ", i, " of ", rmvd1)
		}
	}

}
