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
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"strconv"
)

var (
	lqlLexer = lexer.Must(getRegexpDefinition(`(\s+)` +
		`|(?P<Keyword>(?i)SELECT|FORMAT|SOURCE|WHERE|POSITION|LIMIT|OFFSET|AND|OR|LIKE|CONTAINS|PREFIX|SUFFIX|NOT|{|})` +
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)` +
		`|(?P<String>"([^\\"]|\\.)*"|'([^\\']|\\.)*')` +
		`|(?P<Operator><>|!=|<=|>=|[-+*/%,.=<>()])` +
		`|(?P<Value>[a-zA-Z0-9_\-\\/!@|#$%^&\*+~\.]+)`,
	))
	parser = participle.MustBuild(
		&Select{},
		participle.Lexer(lqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)

	parserExpr = participle.MustBuild(
		&Expression{},
		participle.Lexer(lqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)

	parserSource = participle.MustBuild(
		&Source{},
		participle.Lexer(lqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)
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
		Format   string      `("FORMAT" @String)?`
		Source   *Source     `("SOURCE" @@)?`
		Where    *Expression `("WHERE" @@)?`
		Position *Position   `("POSITION" @@)?`
		Offset   *int64      `("OFFSET" @Value)?`
		Limit    int64       `"LIMIT" @Value`
	}

	Source struct {
		Tags *Tags       `"{" @@ "}"`
		Expr *Expression ` | @@ `
	}

	Tags struct {
		Tags []*Tag ` @@ ( "," @@ )* `
	}

	Tag struct {
		Key   string `@Ident `
		Value string `"=" (@String|@Value|@Ident)`
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
		Operand string `  (@Ident)`
		Op      string ` (@("<"|">"|">="|"<="|"!="|"="|"CONTAINS"|"PREFIX"|"SUFFIX"|"LIKE"))`
		Value   string ` (@String|@Value|@Ident)`
	}

	Position struct {
		PosId string `(@"TAIL"|@"HEAD"|@String|@Ident)`
	}
)

func (i *Int) Capture(values []string) error {
	v, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return err
	}
	if v < 0 {
		return fmt.Errorf("expecting positive integer, but %d", v)
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

func ParseSource(source string) (*Source, error) {
	if len(source) == 0 {
		return nil, nil
	}

	src := &Source{}
	err := parserSource.ParseString(source, src)
	if err != nil {
		return nil, err
	}
	return src, err
}
