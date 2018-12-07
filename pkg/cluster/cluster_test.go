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

package cluster

import (
	"testing"
)

func TestParseHostId(t *testing.T) {
	hid, err := ParseHostId("65533")
	if err != nil || hid != 65533 {
		t.Fatal("Wrong HostId value ", hid, " err=", err)
	}

	_, err = ParseHostId("66533")
	if err == nil {
		t.Fatal("Must be an error, but it is nil")
	}
}
