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

package cursor

import (
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/model/tag"
	"testing"
	"time"
)

func TestGetOrCreate(t *testing.T) {
	p := NewProvider()
	defer p.Shutdown()

	p.JrnlsProvider = &testJrnlsProvider{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}}
	cur, err := p.GetOrCreate(nil, State{Query: "select limit 10"}, true)
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if e, ok := p.curs[cur.Id()]; !ok || e != p.busy {
		t.Fatal("cur=", cur, ", was not found ")
	}
	p.Release(nil, cur)

	cur2, err := p.GetOrCreate(nil, cur.state, false)
	if err != nil || cur2 != cur || p.curs[cur.Id()] != p.busy {
		t.Fatal("err=", err, ", cur2=", cur2, " p.busy=", p.busy, ", p.curs[cur.Id()]=", p.curs[cur.Id()])
	}
	p.Release(nil, cur2)

	s := cur.state
	s.Id++
	cur2, err = p.GetOrCreate(nil, s, true)
	if err != nil || cur2 == cur || p.curs[cur2.Id()] != p.busy || len(p.curs) != 2 || p.busy.Len() != 2 {
		t.Fatal("err=", err, ", cur2=", cur2, ", p.curs[cur2.Id()]=", p.curs[cur2.Id()], ", p.busy=", p.busy, " p.busy.Len()=", p.busy.Len(), ", len(p.curs)=", len(p.curs))
	}
}

func TestRelease(t *testing.T) {
	p := NewProvider()
	defer p.Shutdown()

	p.JrnlsProvider = &testJrnlsProvider{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}}
	cur, err := p.GetOrCreate(nil, State{Id: 1, Query: "select limit 10"}, true)
	if e, ok := p.curs[1]; !ok || e != p.busy || err != nil {
		t.Fatal("cur=", cur, ", was not found or err=", err)
	}

	cur2, err := p.GetOrCreate(nil, State{Id: 2, Query: "select limit 10"}, true)
	if e, ok := p.curs[2]; !ok || e != p.busy || err != nil {
		t.Fatal("cur2=", cur2, ", was not found or err=", err)
	}

	p.Release(nil, cur)
	if e, ok := p.curs[1]; !ok || e != p.busy || p.busy.Len() != 2 || p.busy.Next() != p.curs[2] {
		t.Fatal("Wrong internal list development")
	}

	p.Release(nil, cur2)
	if p.curs[2] != p.busy || p.busy.Len() != 2 || p.busy.Next() != p.curs[1] {
		t.Fatal("Wrong internal list development")
	}
}

func TestSweepByTime(t *testing.T) {
	p := NewProvider()
	p.idleTo = time.Millisecond
	defer p.Shutdown()

	log4g.SetLogLevel("", log4g.DEBUG)

	if err := p.Init(nil); err != nil {
		t.Fatal("p.Init() == ", err)
	}

	tjp := &testJrnlsProvider{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}}
	p.JrnlsProvider = tjp
	cur, err := p.GetOrCreate(nil, State{Id: 1, Query: "select limit 10"}, true)
	if e, ok := p.curs[1]; !ok || e != p.busy || err != nil {
		t.Fatal("cur=", cur, ", was not found or err=", err)
	}

	cur2, err := p.GetOrCreate(nil, State{Id: 2, Query: "select limit 10"}, true)
	if e, ok := p.curs[2]; !ok || e != p.busy || err != nil {
		t.Fatal("cur2=", cur2, ", was not found or err=", err)
	}

	time.Sleep(3 * time.Millisecond)
	if len(p.curs) != 2 || p.busy.Len() != 2 || p.freePoolSz != 0 {
		t.Fatal("no sweeps are expected")
	}

	e := p.busy
	p.Release(nil, cur2)
	time.Sleep(3 * time.Millisecond)
	if len(p.curs) != 1 || p.busy.Len() != 1 || p.free.Len() != 1 || p.free != e || p.freePoolSz != 1 {
		t.Fatal("cur2 must be swept")
	}

	if len(tjp.released) != 1 || tjp.released["j1"] != "j1" {
		t.Fatal("Must be released, but tjp.release=", tjp.released)
	}
}

func TestSweepBySize(t *testing.T) {
	p := NewProvider()
	p.maxCurs = 1
	defer p.Shutdown()

	log4g.SetLogLevel("", log4g.DEBUG)

	p.JrnlsProvider = &testJrnlsProvider{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}}
	cur, err := p.GetOrCreate(nil, State{Id: 1, Query: "select limit 10"}, true)
	if e, ok := p.curs[1]; !ok || e != p.busy || err != nil {
		t.Fatal("cur=", cur, ", was not found or err=", err)
	}

	cur2, err := p.GetOrCreate(nil, State{Id: 2, Query: "select limit 10"}, true)
	if e, ok := p.curs[2]; !ok || e != p.busy || err != nil {
		t.Fatal("cur2=", cur2, ", was not found or err=", err)
	}

	if len(p.curs) != 2 || p.busy.Len() != 2 || p.free.Len() != 0 {
		t.Fatal("no sweeps are expected")
	}
	p.sweepBySize()

	if len(p.curs) != 1 || p.busy.Len() != 1 || p.free.Len() != 0 || p.freePoolSz != 0 {
		t.Fatal("cur must be swept len(p.curs)=", len(p.curs), ", p.busy.Len()=", p.busy.Len())
	}

}
