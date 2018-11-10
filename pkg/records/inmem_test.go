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

package records

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestCheckWithNil(t *testing.T) {
	b, cnt, err := Check(nil)
	if b != nil || cnt != 0 || err != nil {
		t.Fatal("Expecting ok, but b=", b, ", cnt=", cnt, ", err=", err)
	}
}

func TestCheckNotOk(t *testing.T) {
	testCheckNotOk(t, []byte{0, 0, 0})
	testCheckNotOk(t, []byte{0, 0, 0, 1, 0, 1})
	testCheckNotOk(t, []byte{0, 0, 0, 1, 255, 255, 255, 255})
	testCheckNotOk(t, []byte{0, 0, 0, 3, 1, 2, 3, 255, 255, 255, 254, 1, 2})
}

func TestCheckOk(t *testing.T) {
	testCheckOk(t, nil, 0)
	testCheckOk(t, []byte{0, 0, 0, 0}, 1)
	testCheckOk(t, []byte{255, 255, 255, 255}, 0)
	testCheckOk(t, []byte{0, 0, 0, 1, 123, 255, 255, 255, 255, 34}, 1)
	testCheckOk(t, []byte{0, 0, 0, 3, 123, 1, 2}, 1)
	testCheckOk(t, []byte{0, 0, 0, 2, 123, 1, 0, 0, 0, 0, 0, 0, 0, 1, 10}, 3)
}

func testCheckOk(t *testing.T, b []byte, recs int) {
	rb, cnt, err := Check(b)
	if err != nil {
		t.Fatal("expecting err=nil, but err=", err.Error())
	}

	if cnt != recs {
		t.Fatal("expecting ", recs, ", but found ", cnt, " records by the check. b=", b)
	}

	var shb, shrb reflect.SliceHeader
	shb = *(*reflect.SliceHeader)(unsafe.Pointer(&b))
	shrb = *(*reflect.SliceHeader)(unsafe.Pointer(&rb))
	if shb.Data != shrb.Data {
		t.Fatal("allocation happens? Wrong pointers to in memory data expected ", shb.Data, ", but found ", shrb.Data)
	}
}

func testCheckNotOk(t *testing.T, b []byte) {
	_, _, err := Check(b)
	if err == nil {
		t.Fatal("expecting an err but got nil")
	}
}
