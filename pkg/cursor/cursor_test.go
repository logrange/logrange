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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/embed"
	"github.com/logrange/range/pkg/records/journal"
)

func TestNewCursor(t *testing.T) {
	if _, err := newCursor(nil, State{Query: "ddd"}, &testItFactory{}); err == nil {
		t.Fatal("err must not be nil, From expression compilation must fail")
	}

	if _, err := newCursor(nil, State{}, &testItFactory{}); err == nil {
		t.Fatal("err must not be nil, No sources are provided")
	}

	cur, err := newCursor(nil, State{Query: "select limit 10"},
		&testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if _, ok := cur.it.(*model.LogEventIterator); !ok {
		t.Fatal("cur.it must be *model.LogEventIterator")
	}
	if len(cur.jDescs) != 1 || (cur.jDescs["j1"].it.(*testJIterator)).journal != "j1" {
		t.Fatal("Expecting iterator composed from 1 partition")
	}

	cur, err = newCursor(nil, State{Query: "select limit 10"},
		&testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}, "j2=j2": &testJrnl{"j2"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if _, ok := cur.it.(*model.Mixer); !ok {
		t.Fatal("cur.it must be *model.Mixer")
	}
	if len(cur.jDescs) != 2 || (cur.jDescs["j2"].it.(*testJIterator)).journal != "j2" {
		t.Fatal("Expecting iterator composed from 1 partition")
	}
}

func TestCursorClose(t *testing.T) {
	tp := &testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}, "j2=j2": &testJrnl{"j2"}}}
	cur, err := newCursor(nil, State{Query: "select limit 10"}, tp)
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if _, ok := cur.it.(*model.Mixer); !ok {
		t.Fatal("cur.it must be *model.Mixer")
	}
	if len(cur.jDescs) != 2 || (cur.jDescs["j2"].it.(*testJIterator)).journal != "j2" {
		t.Fatal("Expecting iterator composed from 1 partition")
	}

	if len(tp.released) != 0 {
		t.Fatal("Must be no releases yet")
	}
	cur.close()
	if len(tp.released) != 2 || tp.released["j1"] != "j1" || tp.released["j2"] != "j2" {
		t.Fatal("wrong releases")
	}
}

func TestNewCursorWithPos(t *testing.T) {
	cur, err := newCursor(nil, State{Query: "select limit 10", Pos: "tail"},
		&testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if cur.collectPos() != "j1=FFFFFFFFFFFFFFFFFFFFFFFF" {
		t.Fatal("Wrong pos initialized. Expected j1=FFFFFFFFFFFFFFFFFFFFFFFF, but cur.collectPos=", cur.collectPos())
	}

	j1Pos := journal.Pos{0x1234D, 0xABC}
	cur, err = newCursor(nil, State{Query: "select limit 10", Pos: "j1=" + j1Pos.String()},
		&testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	if cur.collectPos() != "j1=000000000001234D00000ABC" {
		t.Fatal("Wrong pos initialized. Expected j1=000000000001234D00000ABC, but cur.collectPos=", cur.collectPos())
	}

	cur, err = newCursor(nil, State{Query: "select limit 10", Pos: "j1=" + j1Pos.String()},
		&testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}, "j2=j2": &testJrnl{"j2"}}})
	if err != nil {
		t.Fatal("err must be nil, but err=", err)
	}
	pos := cur.collectPos()
	t.Log("cur.collectPos=", pos)
	if pos != "j1=000000000001234D00000ABC:j2=000000000000000000000000" && pos != "j2=000000000000000000000000:j1=000000000001234D00000ABC" {
		t.Fatal("Wrong pos initialized. Expected j1=000000000001234D00000ABC:j2=000000000000000000000000, but cur.collectPos=", cur.collectPos())
	}
}

func TestApplyState(t *testing.T) {
	cur, _ := newCursor(nil, State{Query: "select limit 10", Pos: "tail"},
		&testItFactory{j: map[tag.Line]*testJrnl{"j1=j1": &testJrnl{"j1"}}})
	state := cur.state
	err := cur.ApplyState(State{Id: state.Id, Pos: "blah blah"})
	if err == nil || cur.state != state {
		t.Fatal("err must not be nil and new state must not be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

	err = cur.ApplyState(State{Id: state.Id, Query: "select limit 10"})
	if err == nil || cur.state != state {
		t.Fatal("err must not be nil and new state must not be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

	err = cur.ApplyState(State{Id: state.Id, Query: "select limit 10"})
	if err == nil || cur.state != state {
		t.Fatal("err must not be nil and new state must not be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

	err = cur.ApplyState(State{Id: state.Id, Query: "select limit 10", Pos: "j1=000000000001234D00000ABC"})
	if err != nil || cur.state == state {
		t.Fatal("err must be nil and new state must be applied err=", err, " state=", state, " cur.state=", cur.state)
	}

}

func TestCursorOffset(t *testing.T) {
	dir, err := ioutil.TempDir("", "tempJournal")
	defer os.RemoveAll(dir)

	jc, err := embed.NewJCtrlr(embed.JCtrlrConfig{JournalsDir: dir})
	if err != nil {
		t.Fatal("Could not create new controller, err=", err)
	}

	defer jc.Close()

	// Obtaining the journal by its name "teststream"
	j, err := jc.GetOrCreate(context.Background(), "teststream")
	if err != nil {
		t.Fatal("could not get new journal err=", err)
	}

	les := []model.LogEvent{{1, []byte("asdfasdf"), ""}, {2, []byte("asdf"), ""}, {3, []byte("test"), ""}}
	lew := model.NewTestLogEventsWrapper(les)

	// Writes records into the journal
	_, _, err = j.Write(context.Background(), lew)
	if err != nil {
		t.Fatal("Write err=", err)
	}

	j.Sync()

	limit := 2
	offset := 0
	pos := ""

	// forward : first page of size 2
	events, pos, err := readEvents(offset, limit, pos, j)
	if err != nil && err != io.EOF {
		t.Fatal("failed to read events from 0-2", err)
	}

	if !reflect.DeepEqual(events, []string{"asdfasdf", "asdf"}) {
		t.Fatalf("incorrect paginated results: got %v, want %v", events, []string{"asdfasdf", "asdf"})
	}

	// forward : second page of size 2
	events, pos, err = readEvents(offset, limit, pos, j)
	if err != nil && err != io.EOF {
		t.Fatal("failed to read events from 0-2", err)
	}

	if !reflect.DeepEqual(events, []string{"test"}) {
		t.Fatalf("incorrect paginated results: got %v, want %v", events, []string{"test"})
	}

	offset = -1 * limit
	pos = "tail"
	// backward : first page of size 2
	events, pos, err = readEvents(offset, limit, pos, j)
	if err != nil && err != io.EOF {
		t.Fatal("failed to read events from 0-2", err)
	}

	if !reflect.DeepEqual(events, []string{"test", "asdf"}) {
		t.Fatalf("incorrect paginated results: got %v, want %v", events, []string{"test", "asdf"})
	}

	// backward : second page of size 2
	events, pos, err = readEvents(offset, limit, pos, j)
	if err != nil && err != io.EOF {
		t.Fatal("failed to read events from 0-2", err)
	}

	if !reflect.DeepEqual(events, []string{"asdfasdf"}) {
		t.Fatalf("incorrect paginated results: got %v, want %v", events, []string{"asdfasdf"})
	}
}

func readEvents(offset int, limit int, pos string, j journal.Journal) ([]string, string, error) {
	var events []string
	iteratorFactory := &fakeIteratorFactory{j: j}
	ctx := context.Background()
	cur, err := newCursor(nil, State{Query: "select limit 10", Pos: pos}, iteratorFactory)
	if err != nil {
		return nil, "", fmt.Errorf("err must be nil, but err= %v", err)
	}

	cur.Offset(ctx, offset)

	for limit > 0 && err == nil {
		log.Printf("cur.CurrentPos() is %v\n", cur.CurrentPos())
		e, _, err := cur.Get(ctx)
		emsg := e.Msg.MakeCopy()
		if err == nil {
			events = append(events, emsg.AsWeakString())
			limit--
			cur.Next(ctx)
		} else {
			break
		}
	}

	res := cur.commit(ctx)
	return events, res.Pos, nil
}

type fakeIteratorFactory struct {
	j journal.Journal
}

// fakeIteratorFactory
func (itf *fakeIteratorFactory) GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]journal.Journal, error) {
	m := map[tag.Line]journal.Journal{"test=test": itf.j}
	return m, nil
}

func (itf *fakeIteratorFactory) GetJournal(ctx context.Context, src string) (tag.Set, journal.Journal, error) {
	return tag.Set{}, itf.j, nil
}

func (itf *fakeIteratorFactory) Itearator(j journal.Journal, tmRange *model.TimeRange) journal.Iterator {
	return journal.NewJIterator(itf.j)
}

func (itf *fakeIteratorFactory) Release(jn string) {
}
