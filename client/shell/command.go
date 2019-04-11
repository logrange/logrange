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

package shell

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/range/pkg/utils/bytes"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

type (
	command struct {
		name    string
		matcher *regexp.Regexp
		cmdFn   cmdFn
		help    string
	}

	config struct {
		query      []string
		stream     bool
		optKV      string
		beforeQuit func()
		cli        api.Client
	}

	cmdFn func(ctx context.Context, cfg *config) error
)

const (
	cmdSelectName = "select"
	cmdDescName   = "describe"
	cmdTruncName  = "truncate"
	cmdSetOptName = "setoption"
	cmdQuitName   = "quit"
	cmdHelpName   = "help"

	optStreamMode = "stream-mode"

	rgTagsGrp = "tagsCond"
	rgSizeGrp = "size"
)

var commands []command

func init() {
	commands = []command{ //replace with language grammar...
		{
			name:    cmdSelectName,
			matcher: regexp.MustCompile("(?i)^select\\s.+$"),
			cmdFn:   selectFn,
			help:    "run LQL queries, e.g. 'select limit 1'",
		},
		{
			name:    cmdDescName,
			matcher: regexp.MustCompile("(?i)^describe.*$"),
			cmdFn:   descFn,
			help:    "describe source(s), e.g. 'describe tag like \"*a*\"'",
		},
		{
			name:    cmdTruncName,
			matcher: regexp.MustCompile(`(?i)^truncate\s.*$`),
			cmdFn:   truncFn,
			help:    "truncate source(s) if size exceeds a value, e.g. 'truncate name=app,ip=123 maxsize 10.5G'",
		},
		{
			name: cmdSetOptName,
			matcher: regexp.MustCompile("(?i)^(?:(setoption$|setopt$)|(setoption|setopt)\\s+(?P<" +
				cmdSetOptName + ">.+))"),
			cmdFn: setoptFn,
			help:  "set options, e.g. 'setopt stream-mode on'",
		},
		{
			name:    cmdQuitName,
			matcher: regexp.MustCompile("(?i)^(?:quit|exit)$"),
			cmdFn:   quitFn,
			help:    "exit the program",
		},
		{
			name:    cmdHelpName,
			matcher: regexp.MustCompile("(?i)^help$"),
			cmdFn:   helpFn,
			help:    "show help",
		},
	}
}

func execCmd(ctx context.Context, input string, cfg *config) error {
	for _, d := range commands {
		if !d.matcher.MatchString(input) {
			continue
		}
		vars := getInputVars(d.matcher, input)
		cfg.query = []string{input}
		if opt, ok := vars[cmdSetOptName]; ok {
			cfg.optKV = opt
		}
		return d.cmdFn(ctx, cfg)
	}
	return fmt.Errorf("unknown command=%v, try help, or check the syntax.", input)
}

func getInputVars(re *regexp.Regexp, input string) map[string]string {
	match := re.FindStringSubmatch(input)
	varsMap := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i > 0 && i < len(match) {
			varsMap[name] = match[i]
		}
	}
	return varsMap
}

//===================== select =====================

var (
	defaultEvFmtTemplate, _ = model.NewFormatParser("{msg}\n")
)

func selectFn(ctx context.Context, cfg *config) error {
	for _, q := range cfg.query {
		qr, frmt, err := buildReq(q, cfg.stream)
		if err != nil {
			return err
		}

		total := 0
		start := time.Now()
		err = cfg.cli.Select(ctx, qr, cfg.stream,
			func(res *api.QueryResult) {
				printResults(res, frmt, os.Stdout)
				total += len(res.Events)
			})

		if err != nil {
			return err
		}

		fmt.Printf("\ntotal: %d, exec. time %s\n\n", total, time.Now().Sub(start))
	}
	return nil
}

func printResults(res *api.QueryResult, frmt *model.FormatParser, w io.Writer) {
	var (
		le    model.LogEvent
		empty = &api.LogEvent{}
	)

	for _, e := range res.Events {
		if e == nil {
			e = empty
		}

		le.Timestamp = e.Timestamp
		le.Msg = bytes.StringToByteArray(strings.Trim(e.Message, "\n"))
		le.Fields = field.Parse(e.Fields)
		_, _ = w.Write(bytes.StringToByteArray(frmt.FormatStr(&le, e.Tags)))
	}
}

func buildReq(selStr string, stream bool) (*api.QueryRequest, *model.FormatParser, error) {
	l, err := lql.ParseLql(selStr)
	if err != nil {
		return nil, nil, err
	}
	s := l.Select

	fmtt := defaultEvFmtTemplate
	if utils.GetStringVal(s.Format, "") != "" {
		fmtt, err = model.NewFormatParser(*s.Format)
		if err != nil {
			return nil, nil, err
		}
	}

	pos := ""
	if s.Position != nil {
		pos = s.Position.PosId
	}

	lim := utils.GetInt64Val(s.Limit, 50)
	if lim > math.MaxInt32 {
		lim = math.MaxInt32
	}

	waitSec := 0
	if stream {
		waitSec = 10
		lim = math.MaxInt32
	}

	qr := &api.QueryRequest{
		Query:       selStr,
		Pos:         pos,
		Limit:       int(lim),
		WaitTimeout: waitSec,
	}

	return qr, fmtt, nil
}

//===================== describe =====================

func descFn(ctx context.Context, cfg *config) error {
	l, err := lql.ParseLql(cfg.query[0])
	if err != nil {
		return err
	}
	if l.Describe == nil {
		return fmt.Errorf("oops, expected describe, but received %s", cfg.query[0])
	}

	res := &api.SourcesResult{}
	err = cfg.cli.Sources(ctx, l.Describe.Source.String(), res)
	if err != nil {
		return err
	}

	first := true
	for _, s := range res.Sources {
		if first {
			fmt.Printf("\n%10s  %13s  %s", "SIZE", "RECORDS", "TAGS")
			fmt.Printf("\n----------  -------------  ----")
			first = false
		}

		fmt.Printf("\n%10s %13s  %s", humanize.Bytes(s.Size), humanize.Comma(int64(s.Records)), s.Tags)
	}

	if !first {
		if len(res.Sources) < res.Count {
			fmt.Printf("\n\n... and more recods ...\n")
		}

		if res.Count > 1 {
			fmt.Printf("\n----------  -------------")
			fmt.Printf("\n%10s  %13s\n", humanize.Bytes(res.TotalSize), humanize.Comma(int64(res.TotalRec)))
		}
	}

	fmt.Printf("\ntotal: %d sources match the criteria\n\n", res.Count)
	return nil
}

//===================== truncate =====================

func toTruncateRequest(t *lql.Truncate) api.TruncateRequest {
	if t == nil {
		return api.TruncateRequest{}
	}
	return api.TruncateRequest{TagsCond: t.GetTagsCond(), MaxSrcSize: t.GetMaxSize(), MinSrcSize: t.GetMinSize(), OldestTs: t.GetBefore(), DryRun: t.IsDryRun()}
}

func truncFn(ctx context.Context, cfg *config) error {
	l, err := lql.ParseLql(cfg.query[0])
	if err != nil {
		return err
	}

	fmt.Println("understand: ", l.Truncate.String())

	res, err := cfg.cli.Truncate(ctx, toTruncateRequest(l.Truncate))
	if err != nil {
		return err
	}

	if res.Err != nil {
		return fmt.Errorf("server returned the error: %v", res.Err)
	}

	first := true
	for _, s := range res.Sources {
		if first {
			fmt.Printf("\n%12s  %15s  %s", "SIZE(diff)", "RECORDS(diff)", "TAGS")
			fmt.Printf("\n------------  ---------------  ----")
			first = false
		}

		sz := fmt.Sprintf("%s(%s)", humanize.Bytes(s.SizeAfter),
			humanize.Bytes(s.SizeAfter-s.RecordsBefore))
		recs := fmt.Sprintf("%s(%s)", humanize.Comma(int64(s.RecordsAfter)),
			humanize.Comma(int64(s.RecordsAfter-s.RecordsBefore)))
		fmt.Printf("\n%12s %15s  %s", sz, recs, s.Tags)
	}

	fmt.Printf("\n%d sources affected. \n", len(res.Sources))
	return nil
}

//===================== setopt =====================

func setoptFn(_ context.Context, cfg *config) error {
	var (
		opt string
		val string
	)

	keyVal := strings.SplitN(cfg.optKV, " ", 2)
	opt = strings.TrimSpace(strings.ToLower(keyVal[0]))
	if len(keyVal) > 1 {
		val = strings.TrimSpace(strings.ToLower(keyVal[1]))
	}

	switch opt {
	case optStreamMode:
		switch val {
		case "on":
			cfg.stream = true
		case "off":
			cfg.stream = false
		default:
			return fmt.Errorf("unknown value=%v for option=%v", val, opt)
		}
	default:
		return fmt.Errorf("unknown option=%v", opt)
	}

	fmt.Println(keyVal)
	return nil
}

//===================== quit =====================

func quitFn(_ context.Context, cfg *config) error {
	cfg.beforeQuit()
	os.Exit(0)
	return nil
}

//===================== help =====================

func helpFn(_ context.Context, _ *config) error {
	fmt.Printf("\n\t%-10s\n", "[HELP]")
	for _, c := range commands {
		fmt.Printf("\n\t%-15s %s", c.name, c.help)
	}
	fmt.Print("\n\n")
	return nil
}
