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
	"github.com/dustin/go-humanize"
	"github.com/logrange/logrange/pkg/model/tag"
	"strconv"
	"strings"
	"time"
)

var (
	lqlLexer = lexer.Must(getRegexpDefinition(`(\s+)` +
		`|(?P<Keyword>(?i)SELECT|DESCRIBE|TRUNCATE|DRYRUN|BEFORE|ALL|MAXSIZE|MINSIZE|FORMAT|SOURCE|WHERE|POSITION|LIMIT|OFFSET|AND|OR|LIKE|CONTAINS|PREFIX|SUFFIX|NOT)` +
		`|(?P<Ident>[a-zA-Z_][a-z\./\-A-Z0-9_]*)` +
		`|(?P<String>"([^\\"]|\\.)*"|'[^']*')` +
		`|(?P<Operator><>|!=|<=|>=|[-+*/%,.=<>()])` +
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+|[mMkKgGtTbBpP][ib]{0,2})?)` +
		`|(?P<Tags>\{.+\})`,
	))
	parserLql = participle.MustBuild(
		&Lql{},
		participle.Lexer(lqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)

	parserSelect = participle.MustBuild(
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

	parserCondition = participle.MustBuild(
		&Condition{},
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

	Lql struct {
		Select   *Select   `("SELECT" @@)?`
		Describe *Describe `("DESCRIBE" @@)?`
		Truncate *Truncate `("TRUNCATE" @@)?`
	}

	Select struct {
		All      bool        `(@"ALL")?`
		Format   *string     `("FORMAT" @String)?`
		Source   *Source     `("SOURCE" @@)?`
		Where    *Expression `("WHERE" @@)?`
		Position *Position   `("POSITION" @@)?`
		Offset   *int64      `("OFFSET" @Number)?`
		Limit    *int64      `("LIMIT" @Number)?`
	}

	Describe struct {
		Source *Source `(@@)?`
	}

	Truncate struct {
		DryRun  bool      `(@"DRYRUN")?`
		Source  *Source   `(@@)?`
		MinSize *Size     `("MINSIZE" @Number)?`
		MaxSize *Size     `("MAXSIZE" @Number)?`
		Before  *DateTime `("BEFORE" (@String|@Number))?`
	}

	Source struct {
		Tags *TagsVal    ` @Tags`
		Expr *Expression ` | @@ `
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

	Size     uint64
	DateTime uint64
)

func (sz *Size) Capture(values []string) error {
	val, err := humanize.ParseBytes(values[0])
	if err == nil {
		*sz = Size(val)
	}
	return err

}

func (tv *TagsVal) Capture(values []string) error {
	tags, err := tag.Parse(values[0])
	if err == nil {
		tv.Tags = tags
	}
	return err
}

func (dt *DateTime) Capture(values []string) error {
	tm, err := parseTime(values[0])
	if err == nil {
		*dt = DateTime(tm)
	}
	return err
}

func ParseLql(lql string) (*Lql, error) {
	res := &Lql{}
	err := parserLql.ParseString(lql, res)
	if err != nil {
		return nil, err
	}
	return res, err
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

// === Lql

func (l *Lql) String() string {
	if l == nil {
		return ""
	}

	var sb strings.Builder
	l.Select.makeString(&sb)
	l.Describe.makeString(&sb)
	l.Truncate.makeString(&sb)
	return sb.String()
}

// === Select

func (s *Select) makeString(sb *strings.Builder) {
	if s == nil {
		return
	}

	sb.WriteString("SELECT")
	if s.All {
		sb.WriteString(" ALL")
	}

	addStringIfNotEmpty("FORMAT", s.Format, sb)
	if s.Source != nil {
		sb.WriteString(" SOURCE")
		s.Source.makeString(sb)
	}
	if s.Where != nil {
		sb.WriteString(" WHERE")
		s.Where.makeString(sb)
	}
	if s.Position != nil {
		sb.WriteString(" POSITION")
		s.Position.makeString(sb)
	}
	addInt64IfNotEmpty("OFFSET", s.Offset, sb)
	addInt64IfNotEmpty("LIMIT", s.Limit, sb)
}

func addStringIfNotEmpty(pfx string, val *string, sb *strings.Builder) {
	if val == nil || len(*val) == 0 {
		return
	}
	sb.WriteByte(' ')
	sb.WriteString(pfx)
	sb.WriteByte(' ')
	sb.WriteString(strconv.Quote(*val))
}

func addInt64IfNotEmpty(pfx string, val *int64, sb *strings.Builder) {
	if val == nil {
		return
	}
	sb.WriteByte(' ')
	sb.WriteString(pfx)
	sb.WriteByte(' ')
	sb.WriteString(fmt.Sprintf("%d", *val))
}

// === Position

func (p *Position) makeString(sb *strings.Builder) {
	if p == nil {
		return
	}
	sb.WriteByte(' ')
	sb.WriteString(strconv.Quote(p.PosId))
}

// === Condition

func (c *Condition) makeString(sb *strings.Builder) {
	if c == nil {
		return
	}

	sb.WriteByte(' ')
	sb.WriteString(c.Operand)
	sb.WriteByte(' ')
	sb.WriteString(c.Op)
	sb.WriteByte(' ')
	sb.WriteString(strconv.Quote(c.Value))
}

func (c *Condition) String() string {
	var sb strings.Builder
	c.makeString(&sb)
	return sb.String()
}

// === XCondition

func (xc *XCondition) makeString(sb *strings.Builder) {
	if xc == nil {
		return
	}

	if xc.Not {
		sb.WriteString(" NOT")
	}

	if xc.Expr != nil {
		sb.WriteString(" (")
		xc.Expr.makeString(sb)
		sb.WriteString(" )")
		return
	}
	xc.Cond.makeString(sb)
}

// === Expression

func (ex *Expression) makeString(sb *strings.Builder) {
	if ex == nil {
		return
	}
	next := false

	for _, oc := range ex.Or {
		if next {
			sb.WriteString(" OR ")
		}
		next = true
		oc.makeString(sb)
	}
}

// === OrCondition

func (oc *OrCondition) makeString(sb *strings.Builder) {
	if oc == nil {
		return
	}
	next := false

	for _, xc := range oc.And {
		if next {
			sb.WriteString(" AND ")
		}
		next = true
		xc.makeString(sb)
	}
}

// === Source

func (s *Source) makeString(sb *strings.Builder) {
	if s == nil {
		return
	}

	if s.Tags != nil {
		sb.WriteString(" {")
		sb.WriteString(s.Tags.Tags.Line().String())
		sb.WriteByte('}')
	} else {
		s.Expr.makeString(sb)
	}
}

func (src *Source) String() string {
	if src == nil {
		return ""
	}
	var sb strings.Builder
	src.makeString(&sb)
	return sb.String()
}

// === Describe

func (d *Describe) makeString(sb *strings.Builder) {
	if d == nil {
		return
	}

	sb.WriteString("DESCRIBE")
	d.Source.makeString(sb)
}

func (d *Describe) String() string {
	var sb strings.Builder
	d.makeString(&sb)
	return sb.String()
}

// === DateTime

func (dt *DateTime) GetValue() uint64 {
	if dt == nil {
		return 0
	}
	return uint64(*dt)
}

func (dt *DateTime) String() string {
	if dt == nil {
		return ""
	}
	return time.Unix(0, int64(*dt)).String()
}

// === Size

func (sz *Size) GetValue() uint64 {
	if sz == nil {
		return 0
	}
	return uint64(*sz)
}

// === Truncate

func (t *Truncate) GetMaxSize() uint64 {
	return t.MaxSize.GetValue()
}

func (t *Truncate) GetMinSize() uint64 {
	return t.MinSize.GetValue()
}

func (t *Truncate) GetTagsCond() string {
	return t.Source.String()
}

func (t *Truncate) GetBefore() uint64 {
	return t.Before.GetValue()
}

func (t *Truncate) IsDryRun() bool {
	return t.DryRun
}

func (t *Truncate) String() string {
	var sb strings.Builder
	t.makeString(&sb)
	return sb.String()
}

func (t *Truncate) makeString(sb *strings.Builder) {
	if t == nil {
		return
	}

	sb.WriteString("TRUNCATE")
	if t.DryRun {
		sb.WriteString(" DRYRUN")
	}
	t.Source.makeString(sb)
	if t.MinSize != nil {
		p := int64(*t.MinSize)
		addInt64IfNotEmpty("MINSIZE", &p, sb)
	}
	if t.MaxSize != nil {
		p := int64(*t.MaxSize)
		addInt64IfNotEmpty("MAXSIZE", &p, sb)
	}

	if t.Before != nil {
		val := t.Before.String()
		addStringIfNotEmpty("BEFORE", &val, sb)
	}
}
