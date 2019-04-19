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
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/journal"
	"github.com/pkg/errors"
	"io"
	"strings"
)

type (
	// State describes state of a cursor. This structure is used for creating new ones and
	// for providing current state of a cursor
	State struct {
		// Id the cursor state Id
		Id uint64

		// The Query contains the initial query for the cursor.
		Query string

		// Pos indicates the position of the record which must be read next. If it is not empty, it
		// will be applied to the Query
		Pos string
	}

	Cursor interface {
		model.Iterator
		Id() uint64

		// Offset moves the cursor position to backward (negative offset) of forward(positive offset)
		// from the current cursor position
		Offset(ctx context.Context, offs int)
		ApplyState(state State) error
		State(context.Context) State
		WaitNewData(ctx context.Context) error
	}

	// crsr struct describes a context of a query execution. crsr state could be expressed in cursor.State and
	// a new cursor could be created from the struct. crsr supports model.Iterator interface which allows to access
	// to records the cursor selects
	crsr struct {
		logger       log4g.Logger
		state        State
		it           model.Iterator
		jrnlProvider JournalsProvider
		jDescs       map[string]*jrnlDesc
	}

	// JournalsProvider interface is used for creating new cursors. partition.Service most probably implements it.
	JournalsProvider interface {
		// GetJournals returns map of journals by tags:partition.Journal or an error, if any. The returned journals
		// must be released after usage by Release() function
		GetJournals(ctx context.Context, tagsCond *lql.Source, maxLimit int) (map[tag.Line]journal.Journal, error)

		// Release releases the partition. Journal must not be used after the call
		Release(jn string)
	}

	jrnlDesc struct {
		tags tag.Line
		j    journal.Journal
		it   journal.Iterator
	}
)

// newCursor creates the new cursor based on the state provided.
func newCursor(ctx context.Context, state State, jrnlProvider JournalsProvider) (*crsr, error) {
	l, err := lql.ParseLql(state.Query)
	if err != nil || l.Select == nil {
		return nil, errors.Wrapf(err, "could not parse lql=%s, expecting select statement", state.Query)
	}
	sel := l.Select

	srcs, err := jrnlProvider.GetJournals(ctx, sel.Source, 50)

	if err != nil {
		return nil, errors.Wrapf(err, "could not get a list of journals for the query %s", state.Query)
	}

	if len(srcs) == 0 {
		return nil, errors.Errorf("no sources for the expression the query %s", state.Query)
	}

	jd := make(map[string]*jrnlDesc, len(srcs))
	// create the iterators
	var it model.Iterator
	if len(srcs) == 1 {
		for tags, jrnl := range srcs {
			jit := jrnl.Iterator()
			it = (&model.LogEventIterator{}).Wrap(tags, jit)

			jd[jrnl.Name()] = &jrnlDesc{tags, jrnl, jit}
		}
	} else {
		mxs := make([]model.Iterator, len(srcs))

		i := 0
		for tags, jrnl := range srcs {
			jit := jrnl.Iterator()
			jd[jrnl.Name()] = &jrnlDesc{tags, jrnl, jit}
			mxs[i] = (&model.LogEventIterator{}).Wrap(tags, jit)
			i++
		}

		// mixing them
		for len(mxs) > 1 {
			for i := 0; i < len(mxs)-1; i += 2 {
				m := &model.Mixer{}
				m.Init(model.GetEarliest, mxs[i], mxs[i+1])
				mxs[i/2] = m
			}
			if len(mxs)&1 == 1 {
				mxs[len(mxs)/2] = mxs[len(mxs)-1]
				mxs = mxs[:len(mxs)/2+1]
			} else {
				mxs = mxs[:len(mxs)/2]
			}
		}

		it = mxs[0]
	}

	if sel.Where != nil {
		it, err = newFIterator(it, sel.Where)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create filter for %s ", state.Query)
		}
	}

	cur := new(crsr)
	cur.logger = log4g.GetLogger("cursor")
	cur.state = state
	cur.it = it
	cur.jDescs = jd
	cur.jrnlProvider = jrnlProvider
	if err := cur.applyPos(); err != nil {
		return nil, errors.Wrapf(err, "the position %s could not be applied ", state.Pos)
	}

	return cur, nil
}

// String returns the cursor description
func (cur *crsr) String() string {
	return fmt.Sprintf("{descs:%d, state:%s}", len(cur.jDescs), cur.state.String())
}

// Id returns the cursor Id
func (cur *crsr) Id() uint64 {
	return cur.state.Id
}

// Next part of the model.Iterator
func (cur *crsr) Next(ctx context.Context) {
	cur.it.Next(ctx)
}

// Get part of the model.Iterator
func (cur *crsr) Get(ctx context.Context) (model.LogEvent, tag.Line, error) {
	return cur.it.Get(ctx)
}

// Release part of the model.Iterator
func (cur *crsr) Release() {
	cur.it.Release()
}

func (cur *crsr) SetBackward(bkwd bool) {
	cur.it.SetBackward(bkwd)
}

func (cur *crsr) CurrentPos() records.IteratorPos {
	return cur.it.CurrentPos()
}

// Offset moves the cursor position either backward (negative offset) or
// forward (positive offset) value
func (cur *crsr) Offset(ctx context.Context, offs int) {
	if offs == 0 {
		return
	}

	var pos records.IteratorPos
	pos = records.IteratorPosUnknown

	bkwd := false
	if offs < 0 {
		bkwd = true
		offs = -offs
		_, _, err := cur.Get(ctx)
		pos = cur.CurrentPos()
		cur.SetBackward(true)
		if err == io.EOF {
			cur.Get(ctx)
			pos = cur.CurrentPos()
			offs--
		} else {
			cur.iterateToPos(ctx, pos)
		}
	}

	for offs > 0 {
		cur.Next(ctx)
		offs--
		_, _, err := cur.Get(ctx)
		if err != nil {
			pos = records.IteratorPosUnknown
			break
		}
		pos = cur.CurrentPos()
	}

	if bkwd {
		cur.SetBackward(false)
		cur.iterateToPos(ctx, pos)
	}
}

// State returns current the cursor state
func (cur *crsr) State(ctx context.Context) State {
	// calling cur.Get(ctx) to fix the cursor position in case of last call was cur.Next()
	cur.Get(ctx)
	cur.state.Pos = cur.collectPos()
	return cur.state
}

// ApplyState tries to apply state to the cursor. Returns an error, if the operation could not be completed.
// Current implementation allows to apply position only
func (cur *crsr) ApplyState(state State) error {
	if cur.state.Query != state.Query || cur.state.Id != state.Id {
		return errors.Errorf("Could not apply state %s to the current cursor state %s", state, cur.state)
	}

	if cur.state.Pos != state.Pos {
		oldPos := cur.state.Pos
		cur.state.Pos = state.Pos
		err := cur.applyStatePos()
		if err != nil {
			cur.state.Pos = oldPos
			return errors.Wrapf(err, "Could not apply position %s to the cursor state %s ", state.Pos, cur.state)
		}
	}
	return nil
}

// WaitNewData waits for the new data in the cursor. It returns nil if new data is arrived, or
// ctx.Err() otherwise
func (cur *crsr) WaitNewData(ctx context.Context) error {
	cur.it.Release()
	ctx2, cancel := context.WithCancel(ctx)
	for _, it := range cur.jDescs {
		go func(jrnl journal.Journal, pos journal.Pos) {
			jrnl.Chunks().WaitForNewData(ctx2, pos)
			cancel()
		}(it.j, it.it.Pos())
	}
	<-ctx2.Done()
	return ctx.Err()
}

const cPosJrnlSplit = ":"
const cPosJrnlVal = "="

// Commit is called by the cursor reader to indicate that the reading process is over and return the current state
func (cur *crsr) commit(ctx context.Context) State {
	st := cur.State(ctx)
	cur.it.Release()
	return st
}

// collectPos walks over all iterators to collect their current position
func (cur *crsr) collectPos() string {
	var sb strings.Builder
	first := true
	for jn, jd := range cur.jDescs {
		if !first {
			sb.WriteString(cPosJrnlSplit)
		} else {
			first = false
		}
		sb.WriteString(jn)
		sb.WriteString(cPosJrnlVal)
		sb.WriteString(jd.it.Pos().String())
	}
	return sb.String()
}

func (cur *crsr) close() {
	for _, jd := range cur.jDescs {
		jd.it.Close()
		cur.jrnlProvider.Release(jd.j.Name())
		jd.it = nil
		jd.j = nil
	}
	cur.jDescs = nil
}

// iterateToPos is helpful in case of multiple journals are mixed together we will be
// able to get the selected (active) journal position. It is needed for finding
// the position for switching mixer direction forward-backward-forward cause in
// case of multiple journals (mixing them) the position can jump due to the mixer
// specific. The function tries to iterated to the position until it is met.
// handle with extra care.
func (cur *crsr) iterateToPos(ctx context.Context, pos records.IteratorPos) {
	if len(cur.jDescs) <= 1 || pos == records.IteratorPosUnknown {
		return
	}

	for {
		_, _, err := cur.it.Get(ctx)
		if err != nil {
			cur.logger.Warn("Got an error while iterating to the pos=", pos, " for ", cur)
			return
		}

		if cur.it.CurrentPos() == pos {
			return
		}
		cur.Next(ctx)
	}
}

func (cur *crsr) applyPos() error {
	if !cur.applyCornerPos(cur.state.Pos) {
		err := cur.applyStatePos()
		if err != nil {
			return err
		}
	}
	return nil
}

func (cur *crsr) applyCornerPos(pstr string) bool {
	ps := strings.ToLower(pstr)
	var p journal.Pos
	if ps == "tail" {
		p.CId = 0xFFFFFFFFFFFFFFFF
		p.Idx = 0xFFFFFFFF
	} else if ps != "head" && ps != "" {
		return false
	}

	for _, jd := range cur.jDescs {
		jd.it.SetPos(p)
	}
	return true
}

func (cur *crsr) applyStatePos() error {
	vals := strings.Split(cur.state.Pos, cPosJrnlSplit)
	m := make(map[string]journal.Pos, len(vals))
	for _, v := range vals {
		kv := strings.Split(v, cPosJrnlVal)
		if len(kv) != 2 {
			return errors.Errorf(
				"Could not parse position=%s, value the %s sub-string doesn't look like partition pos. Expecting <jrnlId>%s<jrnlPos>",
				cur.state.Pos, v, cPosJrnlVal)
		}

		jrnl := kv[0]
		pos, err := journal.ParsePos(kv[1])
		if err != nil {
			return errors.Wrapf(err, "Could not parse pos %s to partition.Pos", kv[1])
		}
		m[jrnl] = pos
	}

	for j, pos := range m {
		if jd, ok := cur.jDescs[j]; ok {
			jd.it.SetPos(pos)
		}
	}
	return nil
}

func (s State) String() string {
	return fmt.Sprintf("{Id: %d, Query:\"%s\", Pos:%s}", s.Id, s.Query, s.Pos)
}
