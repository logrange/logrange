// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a MakeCopy of the License at
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
	"time"
)

// Record is a file entry (e.g. log line) represented as a slice of bytes
// plus some parsed out (or additional) meta information attached to it.
// Sometimes it's convenient to parse out the needed info from the file entry
// on the client side and not sink, it saves sink resources. Also
// sink may simply not know some information which client knows (e.g.
// containerId).
type Record struct {

	// Raw bytes, representing file entry read
	Data []byte

	// Record date
	Date time.Time

	// TagsCond, attached to the entry
	Tags map[string]string
}

func NewRecord(data []byte, date time.Time) *Record {
	r := &Record{
		Data: data,
		Date: date,
		Tags: make(map[string]string),
	}
	return r
}

func (r *Record) GetData() []byte {
	return r.Data
}

func (r *Record) setData(data []byte) {
	r.Data = data
}

func (r *Record) GetDate() time.Time {
	return r.Date
}

func (r *Record) setDate(t time.Time) {
	r.Date = t
}

func (r *Record) GetTag(k string) string {
	v, _ := r.Tags[k]
	return v
}

func (r *Record) SetTag(k string, v string) {
	r.Tags[k] = v
}
