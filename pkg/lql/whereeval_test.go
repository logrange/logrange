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
	le := &model.LogEvent{Timestamp: 123, Msg: "aaaabbbb"}
	testWhereExpGeneral(t, "msg like \"aaa*\"", le, true)
	testWhereExpGeneral(t, "msg contains ab", le, true)
	testWhereExpGeneral(t, "msg prefix aa", le, true)
	testWhereExpGeneral(t, "msg prefix ab", le, false)
	testWhereExpGeneral(t, "msg suffix ab", le, false)
	testWhereExpGeneral(t, "msg suffix bb", le, true)
	testWhereExpGeneral(t, "ts = 123 and msg suffix bb", le, true)
	testWhereExpGeneral(t, "ts < 123 and msg suffix bb", le, false)
	testWhereExpGeneral(t, "ts < 123 or msg suffix bb", le, true)
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
