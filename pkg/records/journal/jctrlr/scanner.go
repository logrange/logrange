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

package jctrlr

import (
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	scanner struct {
		logger log4g.Logger
	}

	scJournal struct {
		name   string
		dir    string
		chunks []chunk.Id
	}
)

func initScJournal(name, dir string) scJournal {
	return scJournal{name, dir, make([]chunk.Id, 0, 1)}
}

// scan walks through the directory structure, tries to detect journals and
// chunks found them. returns list of the found journals with theirs chunk Ids
func (s *scanner) scan(dir string) ([]scJournal, error) {
	var jrnls []string
	var err error
	chunks := 0

	stt := time.Now()
	defer func() {
		s.logger.Info(len(jrnls), " journals found, ", chunks, " chunks for them. It took ", time.Now().Sub(stt))
	}()

	jrnls, err = scanForJournals(dir)
	if err != nil {
		s.logger.Error("Could not scan dir=", dir, " the err=", err)
		return nil, err
	}

	res := make([]scJournal, 0, len(jrnls))
	for _, jn := range jrnls {
		jDir, err := journalPath(dir, jn)
		if err != nil {
			s.logger.Error("Could not generate journal dir. journal name=", jn, ", err=", err, ", skipping it")
			continue
		}

		cks, err := scanForChunks(jDir)
		if err != nil {
			s.logger.Error("Could not scan for chunks in ", jDir, ", for journal ", jn)
			continue
		}

		sj := scJournal{name: jn, dir: jDir, chunks: cks}
		res = append(res, sj)
		chunks += len(cks)
	}

	return res, nil
}
