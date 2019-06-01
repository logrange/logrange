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

package lql

import (
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/field"
	"testing"
)

func getWhereExpFunc(t *testing.T, exp string) WhereExpFunc {
	e, err := ParseExpr(exp)
	if err != nil {
		t.Fatal("The expression '", exp, "' must be compiled, but err=", err)
	}

	res, err := BuildWhereExpFuncByExpression(e)
	if err != nil {
		t.Fatal("the expression '", exp, "' must be evaluated no problem, but err=", err)
	}

	return res
}

func testWhereExpGeneral(t *testing.T, exp string, le *model.LogEvent, expRes bool) {
	wef := getWhereExpFunc(t, exp)
	if wef(le) != expRes {
		t.Fatal("Expected ", expRes, " for '", exp, "' expression, but got ", !expRes)
	}
}

func TestWhereExpGeneral(t *testing.T) {
	flds, _ := field.NewFields(map[string]string{"f1": "val1", "f2": "val2"})
	le := &model.LogEvent{Timestamp: 123, Msg: []byte("aaaabbbb"), Fields: flds}
	testWhereExpGeneral(t, "msg like \"aaa*\"", le, true)
	testWhereExpGeneral(t, "msg like \"AAA*\"", le, false)
	testWhereExpGeneral(t, "upper(msg) like \"AAA*\"", le, true)
	testWhereExpGeneral(t, "lower(upper(msg)) like \"AAA*\"", le, false)
	testWhereExpGeneral(t, "msg contains ab", le, true)
	testWhereExpGeneral(t, "msg prefix aa", le, true)
	testWhereExpGeneral(t, "msg prefix ab", le, false)
	testWhereExpGeneral(t, "msg suffix ab", le, false)
	testWhereExpGeneral(t, "msg suffix bb", le, true)
	testWhereExpGeneral(t, "ts <= 123 and msg suffix bb", le, true)
	testWhereExpGeneral(t, "ts > 123 ", le, false)
	testWhereExpGeneral(t, "ts < 123 and msg suffix bb", le, false)
	testWhereExpGeneral(t, "ts < 123 or msg suffix bb", le, true)
	testWhereExpGeneral(t, "fields:f1 != aaa", le, true)
	testWhereExpGeneral(t, "fields:f13 != aaa", le, true)
	testWhereExpGeneral(t, "fields:f1 = val1 and fields:f2=val2", le, true)
	testWhereExpGeneral(t, "fields:f1 = VAL1 and fields:f2=val2", le, false)
	testWhereExpGeneral(t, "upper(fields:f1) = VAL1 and fields:f2=val2", le, true)
	testWhereExpGeneral(t, "fields:f1 = val1 and fields:f2=val2 and fields:f3 = \"\"", le, true)
	testWhereExpGeneral(t, "fields:f1 = val1 and fields:f2=val3", le, false)
}

func TestWhereExpPositive(t *testing.T) {
	res, err := BuildWhereExpFuncByExpression(nil)
	if err != nil {
		t.Fatal("Unexpected err=", err)
	}

	if !res(nil) {
		t.Fatal("Must be true")
	}
}
