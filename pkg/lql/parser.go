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
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"github.com/logrange/logrange/pkg/model/tag"
)

var (
	lqlLexer = lexer.Must(getRegexpDefinition(`(\s+)` +
		`|(?P<Keyword>(?i)SELECT|FORMAT|SOURCE|WHERE|POSITION|LIMIT|OFFSET|AND|OR|LIKE|CONTAINS|PREFIX|SUFFIX|NOT)` +
		`|(?P<Ident>[a-zA-Z_][a-z\./\-A-Z0-9_]*)` +
		"|(?P<String>\"([^\\\"]|\\.)*\"|'([^\\']|\\.)*')" +
		`|(?P<Operator><>|!=|<=|>=|[-+*/%,.=<>()])` +
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)` +
		`|(?P<Tags>\{.+\})`,
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
	TagsVal struct {
		Tags tag.Set
	}

	Select struct {
		Tail     bool        `"SELECT" `
		Format   string      `("FORMAT" @String)?`
		Source   *Source     `("SOURCE" @@)?`
		Where    *Expression `("WHERE" @@)?`
		Position *Position   `("POSITION" @@)?`
		Offset   *int64      `("OFFSET" @Number)?`
		Limit    int64       `"LIMIT" @Number`
	}

	Source struct {
		Tags *TagsVal    ` @Tags`
		Expr *Expression ` | @@ `
	}

	Tags struct {
		Tags []*Tag ` @@ ( "," @@ )* `
	}

	Tag struct {
		Key   string `@Ident `
		Value string `"=" (@String|@Ident)`
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
		Value   string ` (@String|@Ident|@Number)`
	}

	Position struct {
		PosId string `(@"TAIL"|@"HEAD"|@String|@Ident)`
	}
)

func (tv *TagsVal) Capture(values []string) error {
	tags, err := tag.Parse(values[0])
	if err == nil {
		tv.Tags = tags
	}
	return err
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
