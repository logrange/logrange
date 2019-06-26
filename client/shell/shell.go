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
	"github.com/logrange/logrange/pkg/utils"
	"github.com/peterh/liner"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

type (
	shell struct {
		cli   api.Client
		hfile string
	}
)

const (
	shellHistoryFileName = ".lr_history"
)

func Query(ctx context.Context, query []string, stream bool, cli api.Client) error {
	inp := ""
	if len(query) > 0 {
		inp = query[0]
	}
	err := execCmd(ctx, inp, &config{
		query:  query,
		stream: stream,
		cli:    cli,
	})

	if err != nil {
		printError(err)
	}
	return nil
}

func Run(cli api.Client) error {
	printLogo()
	newShell(cli, historyFilePath()).run()
	return nil
}

func historyFilePath() string {
	var fileDir = os.TempDir()
	usr, err := user.Current()
	if err == nil {
		fileDir = usr.HomeDir
	}
	return filepath.Join(fileDir, shellHistoryFileName)
}

func printLogo() {
	fmt.Print("" +
		" _                                       \n" +
		"| |___  __ _ _ _ __ _ _ _  __ _ ___      \n" +
		"| / _ \\/ _` | '_/ _` | ' \\/ _` / -_)   \n" +
		"|_\\___/\\__, |_| \\__,_|_|_|\\__, \\___|\n" +
		"       |___/              |___/          \n\n")
}

func printError(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
}

//===================== shell =====================

func newShell(cli api.Client, hFile string) *shell {
	s := new(shell)
	s.cli = cli
	s.hfile = hFile
	return s
}

func (s *shell) run() {
	lnr := liner.NewLiner()
	lnr.SetCtrlCAborts(true)

	s.loadHistory(lnr)
	beforeQuit := func() {
		s.saveHistory(lnr)
		_ = lnr.Close()
		fmt.Println("bye!")
	}

	defer beforeQuit()
	cfg := &config{ //should be shared to allow setopts
		cli:        s.cli,
		beforeQuit: beforeQuit,
	}

	for {
		inp, err := lnr.Prompt("lr>")
		if err != nil {
			printError(err)
			if err == io.EOF || err == liner.ErrPromptAborted {
				break
			}
		}

		inp = strings.TrimSpace(inp)
		if inp == "" {
			continue
		}

		lnr.AppendHistory(inp)
		ctx, cancel := context.WithCancel(context.Background())
		utils.NewNotifierOnIntTermSignal(func(s os.Signal) {
			cancel()
		})

		err = execCmd(ctx, inp, cfg)
		if err != nil {
			printError(err)
		}
	}
}

func (s *shell) loadHistory(lnr *liner.State) {
	f, err := os.OpenFile(s.hfile, os.O_RDONLY|os.O_CREATE, 0640)
	if err != nil {
		printError(err)
		return
	}
	_, err = lnr.ReadHistory(f)
	if err != nil {
		printError(err)
		return
	}
	_ = f.Close()
}

func (s *shell) saveHistory(lnr *liner.State) {
	f, err := os.OpenFile(s.hfile, os.O_WRONLY|os.O_CREATE, 0640)
	if err != nil {
		printError(err)
		return
	}
	_, err = lnr.WriteHistory(f)
	if err != nil {
		printError(err)
		return
	}
	_ = f.Close()
}
