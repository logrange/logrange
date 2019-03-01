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
	"github.com/logrange/logrange/pkg/model/tag"
	"path"
	"strings"
)

type (
	// TagsExpFunc returns true if the provided tag are matched with the expression
	TagsExpFunc func(tags tag.Set) bool

	tagsExpFuncBuilder struct {
		tef TagsExpFunc
	}

	tagsCondExp struct {
		tags tag.Set
	}
)

var positiveTagsExpFunc = func(tag.Set) bool { return true }

// BuildTagsExpFuncByCond receives a condition line and parses it to the TagsExpFunc
// The tagCond could be provided in one of the following 2 forms:
//	- conditions like: name=app1 and ip like '123.*'
// 	- tag-line like: name=app1|ip=123.46.32.44
func BuildTagsExpFunc(tagsCond string) (TagsExpFunc, error) {
	exp, err := ParseExpr(tagsCond)
	if err == nil {
		return buildTagsExpFunc(exp)
	}

	// check whether the tagCond is a TagLine
	tags, err1 := tag.Parse(tagsCond)
	if err1 == nil {
		tc := &tagsCondExp{tags}
		return tc.subsetOf, nil
	}

	return nil, err
}

func (tc *tagsCondExp) subsetOf(tags tag.Set) bool {
	return tc.tags.SubsetOf(tags)
}

// buildTagsExpFunc returns  TagsExpFunc by the expression provided
func buildTagsExpFunc(exp *Expression) (TagsExpFunc, error) {
	if exp == nil {
		return positiveTagsExpFunc, nil
	}

	var teb tagsExpFuncBuilder
	err := teb.buildOrConds(exp.Or)
	if err != nil {
		return nil, err
	}

	return teb.tef, nil
}

func (teb *tagsExpFuncBuilder) buildOrConds(ocn []*OrCondition) error {
	if len(ocn) == 0 {
		teb.tef = positiveTagsExpFunc
		return nil
	}

	err := teb.buildXConds(ocn[0].And)
	if err != nil {
		return err
	}

	if len(ocn) == 1 {
		// no need to go ahead anymore
		return nil
	}

	efd0 := teb.tef
	err = teb.buildOrConds(ocn[1:])
	if err != nil {
		return err
	}
	efd1 := teb.tef

	teb.tef = func(tags tag.Set) bool { return efd0(tags) || efd1(tags) }
	return nil
}

func (teb *tagsExpFuncBuilder) buildXConds(cn []*XCondition) (err error) {
	if len(cn) == 0 {
		teb.tef = positiveTagsExpFunc
		return nil
	}

	if len(cn) == 1 {
		return teb.buildXCond(cn[0])
	}

	err = teb.buildXCond(cn[0])
	if err != nil {
		return err
	}

	efd0 := teb.tef
	err = teb.buildXConds(cn[1:])
	if err != nil {
		return err
	}
	efd1 := teb.tef

	teb.tef = func(tags tag.Set) bool { return efd0(tags) && efd1(tags) }
	return nil

}

func (teb *tagsExpFuncBuilder) buildXCond(xc *XCondition) (err error) {
	if xc.Expr != nil {
		err = teb.buildOrConds(xc.Expr.Or)
	} else {
		err = teb.buildTagCond(xc.Cond)
	}

	if err != nil {
		return err
	}

	if xc.Not {
		efd1 := teb.tef
		teb.tef = func(tags tag.Set) bool { return !efd1(tags) }
		return nil
	}

	return nil
}

func (teb *tagsExpFuncBuilder) buildTagCond(cn *Condition) (err error) {
	op := strings.ToUpper(cn.Op)
	switch op {
	case "<":
		teb.tef = func(tags tag.Set) bool {
			return tags.Tag(cn.Operand) < cn.Value
		}
	case ">":
		teb.tef = func(tags tag.Set) bool {
			return tags.Tag(cn.Operand) > cn.Value
		}
	case "<=":
		teb.tef = func(tags tag.Set) bool {
			return tags.Tag(cn.Operand) <= cn.Value
		}
	case ">=":
		teb.tef = func(tags tag.Set) bool {
			return tags.Tag(cn.Operand) >= cn.Value
		}
	case "!=":
		teb.tef = func(tags tag.Set) bool {
			return tags.Tag(cn.Operand) != cn.Value
		}
	case "=":
		teb.tef = func(tags tag.Set) bool {
			return tags.Tag(cn.Operand) == cn.Value
		}
	case CMP_LIKE:
		// test it first
		_, err := path.Match(cn.Value, "abc")
		if err != nil {
			err = fmt.Errorf("Wrong 'like' expression for %s, err=%s", cn.Value, err.Error())
		} else {
			teb.tef = func(tags tag.Set) bool {
				res, _ := path.Match(cn.Value, tags.Tag(cn.Operand))
				return res
			}
		}
	case CMP_CONTAINS:
		teb.tef = func(tags tag.Set) bool {
			return strings.Contains(tags.Tag(cn.Operand), cn.Value)
		}
	case CMP_HAS_PREFIX:
		teb.tef = func(tags tag.Set) bool {
			return strings.HasPrefix(tags.Tag(cn.Operand), cn.Value)
		}
	case CMP_HAS_SUFFIX:
		teb.tef = func(tags tag.Set) bool {
			return strings.HasSuffix(tags.Tag(cn.Operand), cn.Value)
		}
	default:
		err = fmt.Errorf("Unsupported operation %s for '%s' tag ", cn.Op, cn.Operand)
	}
	return err
}
