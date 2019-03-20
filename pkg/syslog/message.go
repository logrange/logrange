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

package syslog

import (
	"fmt"
	"strings"
	"time"
)

type (
	Message struct {
		Severity Priority
		Facility Priority
		Time     time.Time
		Hostname string
		Tag      string
		Msg      string
	}
)

const (
	timeFmt = "2006-01-02T15:04:05.999999Z07:00"
)

const (
	severityMask = 0x07
	facilityMask = 0xf8
)

func Format(m *Message, nlRepl bool, lenLimit int) string {
	msg := m.Msg
	if nlRepl {
		msg = strings.Replace(msg, "\n", "", -1)
	}

	if lenLimit > 0 && len(msg) > lenLimit {
		msg = fmt.Sprintf("%s... [truncated]\n", msg[:lenLimit])
	}

	pr := (m.Facility & facilityMask) | (m.Severity & severityMask)
	return fmt.Sprintf("<%d>1 %s %s %s - - - %s", pr,
		m.Time.Format(timeFmt), m.Hostname, m.Tag, msg)
}
