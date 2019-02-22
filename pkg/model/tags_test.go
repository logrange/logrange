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
	"reflect"
	"testing"
)

func TestCheckTags(t *testing.T) {
	if _, err := CheckTags(""); err != nil {
		t.Fatal("Expecting err=nil, but err=", err)
	}
	if _, err := CheckTags("a=b"); err != nil {
		t.Fatal("Expecting err=nil, but err=", err)
	}
	if _, err := CheckTags("a=b|d=f"); err != nil {
		t.Fatal("Expecting err=nil, but err=", err)
	}
}

func TestTLNewTags(t *testing.T) {
	tl := TagLine("")
	tags, err := NewTags("")
	if err != nil || tags.tl != tl || len(tags.tm) != 0 {
		t.Fatal("Expecting ok, but err=", err)
	}

	tl = TagLine("wrongvalue")
	tags, err = NewTags("wrongvalue")
	if err == nil {
		t.Fatal("Expecting wrong value, but tags=", tags)
	}

	tl = TagLine("k=value")
	tags, err = NewTags("k=value")
	if err != nil || tags.tl != tl || len(tags.tm) != 1 || tags.tm["k"] != "value" {
		t.Fatal("Expecting ok, but err=", err, " ", tags)
	}
}

func TestTMNewTagMap(t *testing.T) {
	tm := TagMap{}
	tags, err := tm.NewTags()
	if tags.tl != "" || len(tags.tm) != 0 {
		t.Fatal("Expecting ok, but err=", err, " tags=", tags)
	}

	tm = TagMap{"c": "aaa", "a": "cccc"}
	tags, err = tm.NewTags()
	if tags.tl != TagLine("a=cccc|c=aaa") || !reflect.DeepEqual(tags.tm, tm) {
		t.Fatal("Expecting ok, but err=", err, " tags=", tags)
	}
}

func TestMarshalUnmarshalTags(t *testing.T) {
	tags, err := NewTags("k=value|k1=value2")
	if err != nil {
		t.Fatal("could not create tags err=", err)
	}

	if len(tags.tm) != 2 {
		t.Fatal("unexpected tags=", tags)
	}

	res, err := tags.MarshalJSON()
	if err != nil {
		t.Fatal("could not marshal err=", err)
	}

	var tags2 Tags
	err = tags2.UnmarshalJSON(res)
	if err != nil {
		t.Fatal("could not unmarshal err=", err)
	}

	if !reflect.DeepEqual(tags, tags2) {
		t.Fatal("Expected ", tags, ", but got ", tags2)
	}
}
