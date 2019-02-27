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

package cursor

import (
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/range/pkg/records/journal"
	"testing"
)

func TestNewCursor(t *testing.T) {
	if _, err := newCursor(nil, State{Sources: "ddd"}, nil, nil); err == nil {
		t.Fatal("err must not be nil, Source expression compilation must fail")
	}

	if _, err := newCursor(nil, State{}, &testTidxService{}, nil); err == nil {
		t.Fatal("err must not be nil, No sources are provided")
	}

	cur, err := newCursor(nil, State{},
		&testTidxService{map[model.TagLine]string{"j1": "j1"}},
		&testJrnlCtrlr{map[string]*testJrnl{"j1": &testJrnl{"j1"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if _, ok := cur.it.(*model.LogEventIterator); !ok {
		t.Fatal("cur.it must be *model.LogEventIterator")
	}
	if len(cur.jDescs) != 1 || (cur.jDescs["j1"].it.(*testJIterator)).journal != "j1" {
		t.Fatal("Expecting iterator composed from 1 journal")
	}

	cur, err = newCursor(nil, State{},
		&testTidxService{map[model.TagLine]string{"j1": "j1", "j2": "j2"}},
		&testJrnlCtrlr{map[string]*testJrnl{"j1": &testJrnl{"j1"}, "j2": &testJrnl{"j2"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if _, ok := cur.it.(*model.Mixer); !ok {
		t.Fatal("cur.it must be *model.Mixer")
	}
	if len(cur.jDescs) != 2 || (cur.jDescs["j2"].it.(*testJIterator)).journal != "j2" {
		t.Fatal("Expecting iterator composed from 1 journal")
	}
}

func TestNewCursoreWithPos(t *testing.T) {
	cur, err := newCursor(nil, State{Pos: "tail"},
		&testTidxService{map[model.TagLine]string{"j1": "j1"}},
		&testJrnlCtrlr{map[string]*testJrnl{"j1": &testJrnl{"j1"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if cur.collectPos() != "j1=FFFFFFFFFFFFFFFFFFFFFFFF" {
		t.Fatal("Wrong pos initialized. Expected j1=FFFFFFFFFFFFFFFFFFFFFFFF, but cur.collectPos=", cur.collectPos())
	}

	j1Pos := journal.Pos{0x1234D, 0xABC}
	cur, err = newCursor(nil, State{Pos: "j1=" + j1Pos.String()},
		&testTidxService{map[model.TagLine]string{"j1": "j1"}},
		&testJrnlCtrlr{map[string]*testJrnl{"j1": &testJrnl{"j1"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if cur.collectPos() != "j1=000000000001234D00000ABC" {
		t.Fatal("Wrong pos initialized. Expected j1=000000000001234D00000ABC, but cur.collectPos=", cur.collectPos())
	}

	cur, err = newCursor(nil, State{Pos: "j1=" + j1Pos.String()},
		&testTidxService{map[model.TagLine]string{"j1": "j1", "j2": "j2"}},
		&testJrnlCtrlr{map[string]*testJrnl{"j1": &testJrnl{"j1"}, "j2": &testJrnl{"j2"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if cur.collectPos() != "j1=000000000001234D00000ABC:j2=000000000000000000000000" {
		t.Fatal("Wrong pos initialized. Expected j1=000000000001234D00000ABC:j2=000000000000000000000000, but cur.collectPos=", cur.collectPos())
	}
}

func TestApplyState(t *testing.T) {
	cur, _ := newCursor(nil, State{Pos: "tail"},
		&testTidxService{map[model.TagLine]string{"j1": "j1"}},
		&testJrnlCtrlr{map[string]*testJrnl{"j1": &testJrnl{"j1"}}})
	state := cur.state
	err := cur.ApplyState(State{Id: state.Id, Pos: "blah blah"})
	if err == nil || cur.state != state {
		t.Fatal("err must not be nil and new state must not be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

	err = cur.ApplyState(State{Id: state.Id, Sources: "a=b"})
	if err == nil || cur.state != state {
		t.Fatal("err must not be nil and new state must not be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

	err = cur.ApplyState(State{Id: state.Id, Where: "a=b"})
	if err == nil || cur.state != state {
		t.Fatal("err must not be nil and new state must not be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

	err = cur.ApplyState(State{Id: state.Id, Pos: "j1=000000000001234D00000ABC"})
	if err != nil || cur.state == state {
		t.Fatal("err must be nil and new state must be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

}
