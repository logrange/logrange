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

package utils

import (
	"encoding/json"
	"testing"
)

func TestEscapeJsonStr(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "test1", args: struct{ s string }{s: ""}},
		{name: "test2", args: struct{ s string }{s: "\""}},
		{name: "test3", args: struct{ s string }{s: "ha\\\r\"haЛwПР\"" + string(byte(0x19))}},
		{name: "test4", args: struct{ s string }{s: string(byte(0x20)) + "\"\nЫqЛ\"\\ЫЗvz"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb, _ := json.Marshal(tt.args.s)
			exp := string(bb)
			act := EscapeJsonStr(tt.args.s)
			if exp != act {
				t.Fatalf("expected to match: exp=%v and act=%v", exp, act)
			}
		})
	}
}
