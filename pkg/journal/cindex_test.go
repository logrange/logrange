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

package journal

import (
	"encoding/json"
	"github.com/logrange/range/pkg/records/chunk"
	"reflect"
	"testing"
)

func TestMarshal(t *testing.T) {
	v := map[string]sortedChunks{"aaa": {&chkInfo{Id: chunk.NewId(), MinTs: 1234, MaxTs: 1234}}}
	res, err := json.Marshal(v)
	if err != nil {
		t.Fatal("could not marshal v=", v, ", err=", err)
	}

	var v1 map[string]sortedChunks
	err = json.Unmarshal(res, &v1)
	if err != nil {
		t.Fatal("could not unmarshal res=", string(res), ", err=", err)
	}

	if !reflect.DeepEqual(v, v1) {
		t.Fatalf("v=%v, v1=%v", v, v1)
	}
}
