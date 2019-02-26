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

package model

import (
	"context"
	"io"
)

type (
	// SelectF decides which one should be selected, it returns true if ev1
	// must be selected instead of ev2. If ev2 should be used then it
	// returns false
	SelectF func(ev1, ev2 LogEvent) bool

	// Mixer allows to mix 2 Iterators to one. Mixer provides the Iterator interface
	Mixer struct {
		sf       SelectF
		it1, it2 Iterator
		st       byte
		eof1     bool
		eof2     bool
		le1      LogEvent
		le2      LogEvent
	}
)

// GetFirst returns whether the ev1 should be selected first
func GetFirst(ev1, ev2 LogEvent) bool {
	return true
}

// GetEarliest returns whether ev1 has lowest timestamp rather than ev2
func GetEarliest(ev1, ev2 LogEvent) bool {
	return ev1.Timestamp <= ev2.Timestamp
}

// Init initializes the mixer
func (mr *Mixer) Init(sf SelectF, it1, it2 Iterator) {
	mr.sf = sf
	mr.it1 = it1
	mr.it2 = it2
	mr.st = 0
	mr.eof1 = false
	mr.eof2 = false
	mr.le1 = LogEvent{}
	mr.le2 = LogEvent{}
}

// Next is the
// part of Iterator interface
func (mr *Mixer) Next(ctx context.Context) {
	mr.selectState(ctx)
	switch mr.st {
	case 1:
		mr.it1.Next(ctx)
	case 2:
		mr.it2.Next(ctx)
	}
	mr.st = 0
}

// Get is the part of Iterator interface
func (mr *Mixer) Get(ctx context.Context) (LogEvent, error) {
	mr.selectState(ctx)
	switch mr.st {
	case 1:
		return mr.le1, nil
	case 2:
		return mr.le2, nil
	}
	return LogEvent{}, io.EOF
}

func (mr *Mixer) selectState(ctx context.Context) {
	if mr.st != 0 {
		return
	}
	var err error
	if !mr.eof1 {
		mr.le1, err = mr.it1.Get(ctx)
		mr.eof1 = err == io.EOF
	}

	if !mr.eof2 {
		mr.le2, err = mr.it2.Get(ctx)
		mr.eof2 = err == io.EOF
	}

	if mr.eof1 && mr.eof2 {
		mr.st = 3
		return
	}

	if mr.eof1 {
		mr.st = 2
		return
	}

	if mr.eof2 || mr.sf(mr.le1, mr.le2) {
		mr.st = 1
		return
	}
	mr.st = 2
}
