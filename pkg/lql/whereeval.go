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
	"fmt"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/range/pkg/utils/bytes"
	"path"
	"strings"
)

type (
	// WhereExpFunc returns true if the provided LogEvent matches the where condition
	WhereExpFunc func(le *model.LogEvent) bool

	whereExpFuncBuilder struct {
		wef WhereExpFunc
	}
)

var positiveWhereExpFunc = func(*model.LogEvent) bool { return true }

// BuildWhereExpFunc buildw WHERE condition by the condition human readable form like `a=b AND c=d`
func BuildWhereExpFunc(wCond string) (WhereExpFunc, error) {
	exp, err := ParseExpr(wCond)
	if err != nil {
		return nil, err
	}
	return BuildWhereExpFuncByExpression(exp)
}

// BuildWhereExpFuncByExpression builds where function by the Expression provided
func BuildWhereExpFuncByExpression(exp *Expression) (WhereExpFunc, error) {
	if exp == nil {
		return positiveWhereExpFunc, nil
	}

	var web whereExpFuncBuilder
	err := web.buildOrConds(exp.Or)
	if err != nil {
		return nil, err
	}

	return web.wef, nil
}

func (web *whereExpFuncBuilder) buildOrConds(ocn []*OrCondition) error {
	if len(ocn) == 0 {
		web.wef = positiveWhereExpFunc
		return nil
	}

	err := web.buildXConds(ocn[0].And)
	if err != nil {
		return err
	}

	if len(ocn) == 1 {
		// no need to go ahead anymore
		return nil
	}

	efd0 := web.wef
	err = web.buildOrConds(ocn[1:])
	if err != nil {
		return err
	}
	efd1 := web.wef

	web.wef = func(le *model.LogEvent) bool { return efd0(le) || efd1(le) }
	return nil
}

func (web *whereExpFuncBuilder) buildXConds(cn []*XCondition) (err error) {
	if len(cn) == 0 {
		web.wef = positiveWhereExpFunc
		return nil
	}

	if len(cn) == 1 {
		return web.buildXCond(cn[0])
	}

	err = web.buildXCond(cn[0])
	if err != nil {
		return err
	}

	efd0 := web.wef
	err = web.buildXConds(cn[1:])
	if err != nil {
		return err
	}
	efd1 := web.wef

	web.wef = func(le *model.LogEvent) bool { return efd0(le) && efd1(le) }
	return nil

}

func (web *whereExpFuncBuilder) buildXCond(xc *XCondition) (err error) {
	if xc.Expr != nil {
		err = web.buildOrConds(xc.Expr.Or)
	} else {
		err = web.buildCond(xc.Cond)
	}

	if err != nil {
		return err
	}

	if xc.Not {
		efd1 := web.wef
		web.wef = func(le *model.LogEvent) bool { return !efd1(le) }
		return nil
	}

	return nil
}

func getFirstParamName(id *Identifier) string {
	if len(id.Params) == 0 {
		return id.Operand
	}

	return getFirstParamName(id.Params[0])
}

func (web *whereExpFuncBuilder) buildCond(cn *Condition) (err error) {
	fldName := getFirstParamName(cn.Ident)
	op := strings.ToLower(fldName)
	if op == OPND_TIMESTAMP {
		return web.buildTsCond(cn)
	}

	if op == OPND_MESSAGE {
		return web.buildMsgCond(cn)
	}

	if !strings.HasPrefix(op, "fields:") || len(op) < 8 {
		return fmt.Errorf("operand must be ts, msg, or fields:<fieldname> with non-empty fieldname")
	}
	return web.buildFldCond(cn, fldName)
}

func (web *whereExpFuncBuilder) buildTsCond(cn *Condition) error {
	if len(cn.Ident.Params) != 0 {
		return fmt.Errorf("functions are not supported for ts fields, but %s() is provided ", cn.Ident.Operand)
	}

	tt, err := parseLqlDateTime(cn.Value)
	if err != nil {
		return err
	}
	tm := tt.UnixNano()

	switch cn.Op {
	case "<":
		web.wef = func(le *model.LogEvent) bool {
			return le.Timestamp < tm
		}
	case ">":
		web.wef = func(le *model.LogEvent) bool {
			return le.Timestamp > tm
		}
	case "<=":
		web.wef = func(le *model.LogEvent) bool {
			return le.Timestamp <= tm
		}
	case ">=":
		web.wef = func(le *model.LogEvent) bool {
			return le.Timestamp >= tm
		}
	default:
		err = fmt.Errorf("Unsupported operation %s for timetstamp comparison", cn.Op)
	}
	return err
}

// strTransF returns
type strTransF func(str string) string

func buildMsgLeStrFldF(id *Identifier) (strTransF, error) {
	if len(id.Params) == 0 {
		return func(str string) string {
			return str
		}, nil
	}

	fn := strings.ToUpper(id.Operand)
	if len(id.Params) != 1 {
		return nil, fmt.Errorf("only functions with 1 param supported so far, but for %s() %d params provided", id.Operand, len(id.Params))
	}

	inf, err := buildMsgLeStrFldF(id.Params[0])
	if err != nil {
		return nil, err
	}

	switch fn {
	case "UPPER":
		return func(str string) string {
			return strings.ToUpper(inf(str))
		}, nil
	case "LOWER":
		return func(str string) string {
			return strings.ToLower(inf(str))
		}, nil
	}
	return nil, fmt.Errorf("only functions with 1 param supported so far, but for %s() %d params provided", id.Operand, len(id.Params))

}

func (web *whereExpFuncBuilder) buildMsgCond(cn *Condition) (err error) {
	op := strings.ToUpper(cn.Op)
	val := cn.Value
	lsf, err := buildMsgLeStrFldF(cn.Ident)
	if err != nil {
		return err
	}

	switch op {
	case CMP_CONTAINS:
		web.wef = func(le *model.LogEvent) bool {
			return strings.Contains(lsf(bytes.ByteArrayToString(le.Msg)), val)
		}
	case CMP_HAS_PREFIX:
		web.wef = func(le *model.LogEvent) bool {
			return strings.HasPrefix(lsf(bytes.ByteArrayToString(le.Msg)), val)
		}
	case CMP_HAS_SUFFIX:
		web.wef = func(le *model.LogEvent) bool {
			return strings.HasSuffix(lsf(bytes.ByteArrayToString(le.Msg)), val)
		}
	case CMP_LIKE:
		// test it first
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			err = fmt.Errorf("wrong 'like' expression for %s, err=%s", cn.Value, err.Error())
		} else {
			web.wef = func(le *model.LogEvent) bool {
				res, _ := path.Match(cn.Value, lsf(bytes.ByteArrayToString(le.Msg)))
				return res
			}
		}
	default:
		err = fmt.Errorf("unsupported operation \"%s\" for field msg", cn.Op)
	}
	return err
}

func (web *whereExpFuncBuilder) buildFldCond(cn *Condition, fldName string) (err error) {
	// the fldName is prefixed by `fields:`, so cut it
	fldName = fldName[7:]
	op := strings.ToUpper(cn.Op)
	val := cn.Value
	lsf, err := buildMsgLeStrFldF(cn.Ident)
	if err != nil {
		return err
	}

	switch op {
	case CMP_CONTAINS:
		web.wef = func(le *model.LogEvent) bool {
			return strings.Contains(lsf(le.Fields.Value(fldName)), val)
		}
	case CMP_HAS_PREFIX:
		web.wef = func(le *model.LogEvent) bool {
			return strings.HasPrefix(lsf(le.Fields.Value(fldName)), val)
		}
	case CMP_HAS_SUFFIX:
		web.wef = func(le *model.LogEvent) bool {
			return strings.HasSuffix(lsf(le.Fields.Value(fldName)), val)
		}
	case CMP_LIKE:
		// test it first
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			err = fmt.Errorf("uncompilable 'like' expression for \"%s\", expected a shell pattern (not regexp) err=%s", val, err.Error())
		} else {
			web.wef = func(le *model.LogEvent) bool {
				res, _ := path.Match(val, lsf(le.Fields.Value(fldName)))
				return res
			}
		}
	case "=":
		web.wef = func(le *model.LogEvent) bool {
			return lsf(le.Fields.Value(fldName)) == val
		}
	case "!=":
		web.wef = func(le *model.LogEvent) bool {
			return lsf(le.Fields.Value(fldName)) != val
		}
	case ">":
		web.wef = func(le *model.LogEvent) bool {
			return lsf(le.Fields.Value(fldName)) > val
		}
	case "<":
		web.wef = func(le *model.LogEvent) bool {
			return lsf(le.Fields.Value(fldName)) < val
		}
	case ">=":
		web.wef = func(le *model.LogEvent) bool {
			return lsf(le.Fields.Value(fldName)) >= val
		}
	case "<=":
		web.wef = func(le *model.LogEvent) bool {
			return lsf(le.Fields.Value(fldName)) <= val
		}
	default:
		err = fmt.Errorf("unsupport edoperation \"%s\" for field %s", cn.Op, fldName)
	}
	return err
}
