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

package lql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

var (
	kqlLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)SELECT|FORMAT|SOURCE|WHERE|POSITION|LIMIT|OFFSET|AND|OR|LIKE|CONTAINS|PREFIX|SUFFIX|NOT)`+
		`|(?P<Ident>[a-zA-Z0-9-_@#$%?&*{}]+)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Operator><>|!=|<=|>=|[-+*/%,.=<>()])`,
	)), "Keyword"), "String")
	parser     = participle.MustBuild(&Select{}, kqlLexer)
	parserExpr = participle.MustBuild(&Expression{}, kqlLexer)
)

const (
	CMP_CONTAINS   = "CONTAINS"
	CMP_HAS_PREFIX = "PREFIX"
	CMP_HAS_SUFFIX = "SUFFIX"
	CMP_LIKE       = "LIKE"
)

// fixed operands names
const (
	OPND_TIMESTAMP = "ts"
	OPND_MESSAGE   = "msg"
)

type (
	Int int64

	Select struct {
		Tail     bool        `"SELECT" `
		Format   string      `["FORMAT" @String]`
		Source   *Expression `["SOURCE" @@]`
		Where    *Expression `["WHERE" @@]`
		Position *Position   `["POSITION" @@]`
		Offset   *int64      `["OFFSET" @Ident]`
		Limit    int64       `"LIMIT" @Ident`
	}

	Expression struct {
		Or []*OrCondition `@@ { "OR" @@ }`
	}

	OrCondition struct {
		And []*XCondition `@@ { "AND" @@ }`
	}

	XCondition struct {
		Not  bool        ` [@"NOT"] `
		Cond *Condition  `( @@`
		Expr *Expression `| "(" @@ ")")`
	}

	Condition struct {
		Operand string `  (@String|@Ident)`
		Op      string ` (@("<"|">"|">="|"<="|"!="|"="|"CONTAINS"|"PREFIX"|"SUFFIX"|"LIKE"))`
		Value   string ` (@Ident|@String)`
	}

	Position struct {
		PosId string `(@"TAIL"|@"HEAD"|@String|@Ident)`
	}
)

// unquote receives a string and removes either single or double quotes if the
// input has them:
// unquote(" 'aaa'  ") => "aaa"
// unquote(" 'aaa\"  ") => " 'aaa\"  "
// unquote(" 'aaa'") => "aaa"
func unquote(s string) string {
	s1 := strings.Trim(s, " ")
	if len(s1) >= 2 {
		if c := s1[len(s1)-1]; s1[0] == c && (c == '"' || c == '\'') {
			return s1[1 : len(s1)-1]
		}
	}
	return s
}

func (i *Int) Capture(values []string) error {
	v, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return err
	}
	if v < 0 {
		return errors.New(fmt.Sprint("Expecting positive integer, but ", v))
	}
	*i = Int(v)
	return nil
}

func Parse(lql string) (*Select, error) {
	sel := &Select{}
	err := parser.ParseString(lql, sel)
	if err != nil {
		return nil, err
	}
	return sel, err
}

func ParseExpr(where string) (*Expression, error) {
	if len(where) == 0 {
		return nil, nil
	}

	exp := &Expression{}
	err := parserExpr.ParseString(where, exp)
	if err != nil {
		return nil, err
	}
	return exp, err
}
