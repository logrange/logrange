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

package model

import (
	"context"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"io"
)

type (
	// SelectF decides which one should be selected, it returns true if ev1
	// must be selected instead of ev2. If ev2 should be used then it
	// returns false
	SelectF func(ev1, ev2 LogEvent) bool

	// Mixer allows to mix 2 Iterators to one. Mixer provides the Iterator interface
	Mixer struct {
		sf         SelectF
		src1, src2 srcDesc
		st         byte
		bkwd       bool
	}

	srcDesc struct {
		it   Iterator
		eof  bool
		le   LogEvent
		tags tag.Line
	}
)

// GetEarliest returns whether ev1 has lowest timestamp rather than ev2
func GetEarliest(ev1, ev2 LogEvent) bool {
	return ev1.Timestamp <= ev2.Timestamp
}

// Init initializes the mixer
func (mr *Mixer) Init(sf SelectF, it1, it2 Iterator) {
	mr.sf = sf
	mr.src1 = srcDesc{it: it1, eof: false, le: LogEvent{}, tags: ""}
	mr.src2 = srcDesc{it: it2, eof: false, le: LogEvent{}, tags: ""}
	mr.st = 0
}

// Next is the
// part of Iterator interface
func (mr *Mixer) Next(ctx context.Context) {
	mr.selectState(ctx)
	switch mr.st {
	case 1:
		mr.src1.it.Next(ctx)
		mr.src1.le.Release()
	case 2:
		mr.src2.it.Next(ctx)
		mr.src2.le.Release()
	}
	mr.st = 0
}

// Get is the part of Iterator interface
func (mr *Mixer) Get(ctx context.Context) (LogEvent, tag.Line, error) {
	err := mr.selectState(ctx)
	if err != nil {
		return LogEvent{}, "", err
	}
	switch mr.st {
	case 1:
		return mr.src1.le, mr.src1.tags, nil
	case 2:
		return mr.src2.le, mr.src2.tags, nil
	}
	return LogEvent{}, "", io.EOF
}

func (mr *Mixer) Release() {
	mr.src1.it.Release()
	if mr.st == 1 {
		mr.src1.le.MakeItSafe()
	}
	mr.src1.eof = false

	mr.src2.it.Release()
	if mr.st == 2 {
		mr.src2.le.MakeItSafe()
	}
	mr.src2.eof = false
	if mr.st == 3 {
		mr.st = 0
	}
}

func (mr *Mixer) SetBackward(bkwd bool) {
	if mr.bkwd == bkwd {
		return
	}
	mr.bkwd = bkwd
	mr.src1.it.SetBackward(bkwd)
	mr.src2.it.SetBackward(bkwd)
	mr.Release()
	mr.st = 0
}

func (mr *Mixer) CurrentPos() records.IteratorPos {
	if mr.st == 1 {
		return mr.src1.it.CurrentPos()
	}

	if mr.st == 2 {
		return mr.src2.it.CurrentPos()
	}

	return records.IteratorPosUnknown
}

func (mr *Mixer) selectState(ctx context.Context) error {
	if mr.st != 0 {
		return nil
	}
	var err error
	if !mr.src1.eof {
		mr.src1.le, mr.src1.tags, err = mr.src1.it.Get(ctx)
		if err != nil {
			if err != io.EOF {
				return err
			}
			mr.src1.eof = true
		}
	}

	if !mr.src2.eof {
		mr.src2.le, mr.src2.tags, err = mr.src2.it.Get(ctx)
		if err != nil {
			if err != io.EOF {
				return err
			}
			mr.src2.eof = true
		}
	}

	if mr.src1.eof && mr.src2.eof {
		mr.st = 3
		return nil
	}

	if mr.src1.eof {
		mr.st = 2
		return nil
	}

	if mr.src2.eof || mr.testFunc() {
		mr.st = 1
		return nil
	}
	mr.st = 2
	return nil
}

func (mr *Mixer) testFunc() bool {
	res := mr.sf(mr.src1.le, mr.src2.le)
	if mr.bkwd {
		return !res
	}
	return res
}
