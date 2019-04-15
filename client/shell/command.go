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
	"sort"
	"strings"
	"time"
)

type (
	command struct {
		name    string
		matcher *regexp.Regexp
		cmdFn   cmdFn
	}

	commandHelp struct {
		short string
		long  string
	}

	config struct {
		query      []string
		stream     bool
		optKV      map[string]string
		beforeQuit func()
		cli        api.Client
	}

	cmdFn func(ctx context.Context, cfg *config) error
)

const (
	cmdSelectName = "select"
	cmdSetOptName = "setoption"
	cmdQuitName   = "quit"
	cmdHelpName   = "help"

	optStreamMode = "stream-mode"
)

var commands []command = []command{ //replace with language grammar...
	{
		name:    cmdSelectName,
		matcher: regexp.MustCompile("(?i)^select\\s.+$"),
		cmdFn:   selectFn,
	},
	{
		name: cmdSetOptName,
		matcher: regexp.MustCompile("(?i)^(?:(setoption$|setopt$)|(setoption|setopt)\\s+(?P<" +
			cmdSetOptName + ">.+))"),
		cmdFn: setoptFn,
	},
	{
		name:    cmdQuitName,
		matcher: regexp.MustCompile("(?i)^(?:quit|exit)$"),
		cmdFn:   quitFn,
	},
	{
		name: cmdHelpName,
		matcher: regexp.MustCompile("(?i)^(?:(help$)|(help)\\s+(?P<" +
			cmdHelpName + ">.+))"),
		cmdFn: helpFn,
	},
}

var helps map[string]commandHelp = map[string]commandHelp{
	cmdSelectName: {
		short: "retrieving records from one or multiple partitions, e.g. 'select limit 1'",
		long: `SELECT statement has the following syntax:

	SELECT [<format string>] 
		[FROM ({<tags>}|<tags expression)] 
		[WHERE <fields expression>] 
		[POSITION (head|tail|<specific pos>)] 
		[OFFSET <number>]
		[LIMIT <number>]

The <format string> is a template for formatting resulted records. The
template should be placed in double qotes with variables placed in curly braces. 
The curly braces '{' and '}' could be escaped by "{{" for '{' and "{}" for '}'. 
For example, the template "Record content: {msg}\n" will put the prefix 
"Record content: " before each record's message. The following variables
could be used in the template string:
	ts			- record's timestamp
	ts.<format> - record's timestamp formatted according the <format> (see GoLang time formatting)
	msg 		- record's message
	msg.json 	- record's message escaped and placed in double quotes
	vars		- record's tags and optional fields put together
	vars:<name> - record's field or tag value given by the name provided.
For example: "{ts:2006-01-02} {vars:source} {{{msg}{}"
Default value is "{msg}\n"

The FROM statement allows to define partitions where records will come from. The
FROM statement could be a list of tags to identify the partition uniquely. This case
the list of tags should be placed in curly braces e.g. '... FROM {name=app1,ns=system} ...'
Also the FROM statement can contain an expression with tags for selecting multiple
partitions e.g. '... FROM name="app1" OR ns="system" ...' - select partitions with
tag name equals to "app1" or partitions with tag ns equals to "system"
Default value is "", what matches to all partitions.

The WHERE statement allows to define an exression to filter records. The WHERE statement
can contain mandatory record fields 'ts' and 'msg' or an optional record fields prefixed 
by 'fields:'. For example:
" ... WHERE msg CONTAINS 'ERROR' OR fields.source LIKE 'system*' ... ". 
Default value is "", what matches to any record

The POSITION part defines the starting position where records should be read from 
It could be 'head', 'tail' or a string which could be received from previous 
requests. Default value is 'head'

The OFFSET <number> allows to skip the <number> of records starting from the position.
Default value is 0.

The LIMIT <number> allows to set the number of records in the result returned. 
It doesn't make sense in stream-mode=on. Default value is 0
`,
	},
	"show partitions": {
		short: "allows to show paritions by an expression e.g. 'show partitions name=\"app2\"'",
		long: `SHOW PARTITIONS has the following syntax:

	SHOW PARTITIONS [({<tags>}|<tags expression)][OFFSET <number>][LIMIT <number>]

SHOW PARTITIONS allows to list all partitions which tags match the expression
provided. The partitions will be ordered by their size, so paging is available

OFFSET allows to skip a number of partitions from the top of the list. Default
value is 0

LIMIT sets the number of partitions that should be shown. Default value is 
infinity, so all partitions will be shown.
`,
	},
	cmdHelpName: {
		short: "provides some information about known commands e.g. 'help select'",
		long: `HELP has the following syntax:

	HELP [<command>]

HELP prints information about the command provided. It just prints the list of 
known commands if the command is empty.
`,
	},
	cmdSetOptName: {
		short: "allows to change an option setting. e.g. 'setoption stream-mode on'",
		long: `SETOPTION has the following syntax:

	SETOPTION stream-mode (on|off)

stream-mode allows to enable (stream-mode on) or disable (stream-mode off) the
streaming mode. In streaming mode records are retrieved until end of the stream
is reached. When the end is reached the read of records will be blocked until
new records appear in the stream.
`,
	},
	cmdQuitName: {
		short: "exit from the shell.",
		long: `EXIT or QUIT allows to quit from the shell.
`,
	},
	"show pipes": {
		short: "show the list of known pipes. e.g. 'show pipes'",
		long: `SHOW PIPES has the following syntax:

	SHOW PIPES [OFFSET <number>][LIMIT <number>]

SHOW PIPES lists all known pipes in alphabetical order. Pagination is available.

OFFSET if greater than 0, allows to skip the number of records from the top. 
Default value is 0

LIMIT allows to set the number of records to be shown. Default value is infinity, 
so all pipes will be shown.
`,
	},
	"describe partition": {
		short: "show the details about partition by the tags. e.g. 'describe partition name=app1,ns=system'",
		long: `DESCRIBE PARTITION has the following syntax:

	DESCRIBE PARTITION {<tags>}

DESCRIBE PARTITION prints detailed information about a partition, which can be 
found by the tags provided.
`,
	},
	"describe pipe": {
		short: "show details about the pipe by its name. e.g. 'describe pipe forwarders'",
		long: `DESCRIBE PIPE has the following syntax:

	DESCRIBE PIPE <pipe name>

DESCRIBE PIPE prints detailed information about a pipe by its name
`,
	},
	"truncate": {
		short: "deletes data from partition(s). e.g. 'truncate minsize 10G maxsize 1Tb'",
		long: `TRUNCATE has the following syntax:

	TRUNCATE [DRYRUN] [({<tags>}|<tags expression)][MINSIZE <size>][MAXSIZE <size>][BEFORE <timestamp>]

The TRUNCATE allows to remove part or ALL data from one or many partitions. Partitions 
consist of chunks, so when a partition is truncated one or many chunks are removed
from the partition. There is no way to remove only one record, unless the removed
chunk contains only one record. The partition's chunks are ordered - oldest chunks 
come first. A partition, if it is truncated, always lost oldest chunks. The 
parameters of the TRUNCATE command define which chunks should be deleted

The <size> parameter can be provided in bytes (1000000) or in a human readable
short form like kilobytes (1000k), megabytes (1M), gigabytes etc.notations could 
be used as well.

DRYRUN allows to run truncation in test mode without actual truncation. The flag
is helpful to see what data will be actually removed if the flag is not provided.
Default value is FALSE(!)

({<tags>}|<tags expression) allows to specify the condition which partitions will be
affected. The default value is empty string, what means ALL partitions(!)

MINSIZE allows to set the bar to minimum partition size. The flag value does not
allow to truncate a partion which will follow to the partition size less than
the value provided. For example if the initial partition size is 100G and the 
flag is set to 50G the partition cannot be truncated less than the size (50G).
Default value is 0

MAXSIZE allows to set the parition(s) size to be be considered. A partition must
be at least the MAXSIZE provided to be truncated. Default value is 0.

BEFORE allows to specify the timestamp of chunk records to be truncated. Only chunks
where ALL records timestamped before the provided value, could be removed. 
Default value is 0

Note. TRUNCATE with using both MAXSIZE and BEFORE flags provided, are composed by
logical OR condition. It means that a partition's chunks will be removed either they
meet MAXSIZE or BEFORE criteria, but NOT BOTH OF THEM.
`,
	},
	"create pipe": {
		short: "creates new pipe. e.g. 'create pipe errors where msg contains \"ERROR\"'",
		long: `CREATE PIPE has the following syntax:

	CREATE PIPE <pipe name> [FROM ({<tags>}|<tags expression)] [WHERE <fields expression>]

CREATE PIPE allows to create a new pipe. Pipe is a hook on a write records
to a partition operation. Pipes allow to write some hooked records into  
a piped partition, what could be helpful for futher processing them out 
of there. An obvious example could be to collect ALL records that contains 
"ERROR" word to a special partition. All records that meet the
pipe criteria ("ERROR" word in the message) will be also written in the 
piped partition, where they can be easily find later.

FROM statement allows to specify filter for paiped partitions by the the
tags criteria. Default value is empty, what corresponds to any partition.

WHERE condition allows to set up a filter for records that should be piped.
Default value is empty condition, what means - any record. 

For example, the statement 'create pipe forwarder' creates the new pipe with
name 'forwarder' which allows to write all records that will be written in 
any partition to write to the piped partion as well. 

The piped partition has the tag {logrange.pipe=<pipe name>}. It contains
ONLY records which were written after the pipe had been created.

`,
	},
	"delete pipe": {
		short: "deletes pipe. e.g. 'delete pipe errors'",
		long: `DELETE PIPE has the following syntax:

	DELETE PIPE <pipe name>

DELETE PIPE removes the pipe and stop writing to the piped partition. 
The operation doesn't affect the target partion (tagged by 
{logrange.pipe=<pipe name>}), but just removes the pipe. 
`,
	},
}

var sortedHelps []string

func init() {
	sortedHelps = make([]string, 0, len(helps))
	for c := range helps {
		sortedHelps = append(sortedHelps, c)
	}

	sort.Strings(sortedHelps)
}

func execCmd(ctx context.Context, input string, cfg *config) error {
	for _, d := range commands {
		if !d.matcher.MatchString(input) {
			continue
		}
		cfg.optKV = getInputVars(d.matcher, input)
		return d.cmdFn(ctx, cfg)
	}
	cfg.query = []string{input}
	return exec(ctx, cfg)
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

// ===================== select =====================

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
		err = doSelect(ctx, qr, cfg.cli, cfg.stream,
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

func doSelect(ctx context.Context, qr *api.QueryRequest, cli api.Client, streamMode bool,
	handler func(res *api.QueryResult)) error {

	limit := qr.Limit
	timeout := qr.WaitTimeout
	for ctx.Err() == nil {
		qr.Limit = limit
		qr.WaitTimeout = timeout

		res := &api.QueryResult{}
		err := cli.Query(ctx, qr, res)
		if err != nil {
			return err
		}

		if len(res.Events) != 0 {
			handler(res)
		}
		qr = &res.NextQueryRequest

		if !streamMode {
			if limit <= 0 || len(res.Events) == 0 {
				break
			}
			limit -= len(res.Events)
		}
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

	lim := utils.GetInt64Val(s.Limit, 0)
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

// ===================== describe =====================
func exec(ctx context.Context, cfg *config) error {
	res, err := cfg.cli.Execute(ctx, api.ExecRequest{cfg.query[0]})
	if err != nil {
		return err
	}

	if res.Err != nil {
		fmt.Printf("Execution error(%s): %s\n\n", cfg.query[0], res.Err)
		return nil
	}

	fmt.Println(res.Output)
	return nil
}

// ===================== setopt =====================
func setoptFn(_ context.Context, cfg *config) error {
	val := strings.Trim(strings.ToLower(cfg.optKV[cmdSetOptName]), " ")
	res := strings.Split(val, " ")
	if len(res) != 2 {
		return fmt.Errorf("Unknown option: \"%s\"", val)
	}

	if res[0] != optStreamMode {
		return fmt.Errorf("Unknown option name: \"%s\"", res[0])
	}

	switch res[1] {
	case "on":
		cfg.stream = true
	case "off":
		cfg.stream = false
	default:
		return fmt.Errorf("unknown value=%v for option=%s", val, optStreamMode)
	}

	fmt.Sprintf("%s=%s\n\n", optStreamMode, val)
	return nil
}

// ===================== quit =====================
func quitFn(_ context.Context, cfg *config) error {
	cfg.beforeQuit()
	os.Exit(0)
	return nil
}

// ===================== help =====================
func helpFn(_ context.Context, cfg *config) error {
	cmd, ok := cfg.optKV[cmdHelpName]
	cmd = strings.Trim(strings.ToLower(cmd), " ")
	if !ok || len(cmd) == 0 {
		fmt.Printf("\n\t%-10s\n", "[HELP]")
		for _, c := range sortedHelps {
			d := helps[c]
			fmt.Printf("\n\t%-18s %s", c, d.short)
		}
		fmt.Printf("\n\n")
		return nil
	}

	d, ok := helps[cmd]
	if !ok {
		return fmt.Errorf("Uknown command \"%s\"", cmd)
	}

	fmt.Printf("%s\n", d.long)
	return nil
}
