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

package cli

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/lql"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"text/template"
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
		desc       string
		optKV      string
		beforeQuit func()
		cli        *client
	}

	cmdFn func(ctx context.Context, cfg *config) error
)

const (
	cmdSelectName = "select"
	cmdDescName   = "describe"
	cmdSetOptName = "setoption"
	cmdQuitName   = "quit"
	cmdHelpName   = "help"

	optStreamMode = "stream-mode"
)

var commands []command

func init() {
	commands = []command{
		{
			name:    cmdSelectName,
			matcher: regexp.MustCompile("(?P<" + cmdSelectName + ">(?i)^(?:select$|select\\s.+$))"),
			cmdFn:   selectFn,
			help:    "run LQL queries, e.g. 'select limit 1'",
		},
		{
			name: cmdDescName,
			matcher: regexp.MustCompile("(?i)^(?:(?:describe$|desc$)|(?:describe|desc)\\s+(?P<" +
				cmdDescName + ">.+))"),
			cmdFn: descFn,
			help:  "describe LQL sources, e.g. 'desc tag like \"*a*\"'",
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
		if s, ok := vars[cmdSelectName]; ok {
			cfg.query = []string{s}
		}
		if d, ok := vars[cmdDescName]; ok {
			cfg.desc = d
		}
		if opt, ok := vars[cmdSetOptName]; ok {
			cfg.optKV = opt
		}
		return d.cmdFn(ctx, cfg)
	}
	return fmt.Errorf("unknown command=%v", input)
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
	defaultEvFmtTemplate = template.Must(template.New("default").
		Parse("time={{.Timestamp}}, message={{.Message}}, tags={{.Tags}}\n"))
)

func selectFn(ctx context.Context, cfg *config) error {
	for _, q := range cfg.query {
		qr, frmt, err := buildReq(q, cfg.stream)
		if err != nil {
			return err
		}

		total := 0
		err = cfg.cli.doSelect(ctx, qr, cfg.stream,
			func(res *api.QueryResult) {
				printResults(res, frmt, os.Stdout)
				total += len(res.Events)
			})

		if err != nil {
			return err
		}

		fmt.Printf("\ntotal: %d\n\n", total)
	}
	return nil
}

func printResults(res *api.QueryResult, frmt *template.Template, w io.Writer) {
	empty := &api.LogEvent{}
	for _, e := range res.Events {
		if e == nil {
			e = empty
		}

		e.Message = strings.Trim(e.Message, "\n")
		_ = frmt.Execute(w, e)
	}
}

func buildReq(selStr string, stream bool) (*api.QueryRequest, *template.Template, error) {
	s, err := lql.Parse(selStr)
	if err != nil {
		return nil, nil, err
	}

	fmtt := defaultEvFmtTemplate
	if s.Format != "" {
		fmtt, err = template.New("").Parse(s.Format)
		if err != nil {
			return nil, nil, err
		}
	}

	pos := ""
	if s.Position != nil {
		pos = s.Position.PosId
	}

	lim := s.Limit
	if s.Limit > math.MaxInt32 {
		lim = math.MaxInt32
	}

	waitSec := 0
	if stream {
		waitSec = 1
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
	_, err := lql.ParseExpr(cfg.desc)
	if err != nil {
		return err
	}

	res, err := cfg.cli.doDescribe(ctx, cfg.desc)
	if err != nil {
		return err
	}

	if len(res.Sources) < res.Count {
		fmt.Printf("... and more ...\n")
	}

	fmt.Printf("\ntotal: %d\n\n", res.Count)
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
