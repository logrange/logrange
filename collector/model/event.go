// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
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
	"encoding/json"
	"github.com/logrange/logrange/collector/utils"
)

// Event is a structure which contains a list of records,
// parsed from a file. Event has a consumption confirmation mechanism,
// consumer must call Confirm() as soon as he finished handling the even.
type (
	Event struct {
		// File contains filename of the file from where the records come
		File string

		// Records are the list of parsed records
		Records []*Record

		// Tags information, attached to the list of records
		Meta Meta

		// confCh is a signaling channel, to notify scanner that even was handled
		confCh chan struct{}
	}

	Meta struct {
		SourceId string
		Tags     map[string]string
	}
)

func NewEvent(file string, recs []*Record, meta Meta, confCh chan struct{}) *Event {
	return &Event{
		File:    file,
		Records: recs,
		Meta:    meta,
		confCh:  confCh,
	}
}

func (e *Event) MarshalJSON() ([]byte, error) {
	type alias Event
	return json.Marshal(&struct {
		*alias
		Records int
		Meta    Meta
	}{
		alias:   (*alias)(e),
		Records: len(e.Records),
		Meta:    e.Meta,
	})
}

// consumer must call Confirm() as soon as the event is handled
func (e *Event) Confirm() bool {
	var ok bool
	if e.confCh != nil {
		_, ok = <-e.confCh
		e.confCh = nil
	}
	return ok
}

func (e *Event) String() string {
	return utils.ToJsonStr(e)
}
