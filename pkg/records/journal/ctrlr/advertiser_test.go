// Copyright 2018 The logrange Authors
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

package ctrlr

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/cluster/model"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type jrnlCatalogTest struct {
	repTO time.Duration
	amap  map[string][]chunk.Id
}

func (jct *jrnlCatalogTest) GetJournalInfo(ctx context.Context, jname string) (model.JournalInfo, error) {
	return model.JournalInfo{}, nil
}

func (jct *jrnlCatalogTest) ReportLocalChunks(ctx context.Context, jname string, chunks []chunk.Id) error {
	if jct.repTO > 0 {
		time.Sleep(jct.repTO)
	}
	jct.amap[jname] = chunks
	return nil
}

func newJrnlCatalogTest() *jrnlCatalogTest {
	jct := new(jrnlCatalogTest)
	jct.amap = make(map[string][]chunk.Id)
	return jct
}

func TestAdvertiserAdvertise(t *testing.T) {
	jct := newJrnlCatalogTest()
	a := newAdvertiser(jct)
	c1 := []chunk.Id{1}
	c2 := []chunk.Id{2}
	a.advertise("test", c1)
	time.Sleep(time.Millisecond)
	if len(jct.amap) != 1 || !reflect.DeepEqual(c1, jct.amap["test"]) {
		t.Fatal("Must be reported, but ", jct.amap)
	}

	a.advertise("test1", c1)
	a.advertise("test2", c2)
	time.Sleep(time.Millisecond)
	if len(jct.amap) != 3 || !reflect.DeepEqual(c2, jct.amap["test2"]) {
		t.Fatal("Must be reported, but ", jct.amap)
	}

	a.close()
	a.advertise("test4", c1)
	time.Sleep(time.Millisecond)
	if len(jct.amap) != 3 || jct.amap["test4"] != nil {
		t.Fatal("Must be reported, but ", jct.amap)
	}
}
