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

package journal

import (
	"context"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tindex"
	rjournal "github.com/logrange/range/pkg/records/journal"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/pkg/errors"
)

type (
	// Service struct provides functionality and some functions to work with LogEvent journals
	Service struct {
		Pool     *bytes.Pool         `inject:""`
		Journals rjournal.Controller `inject:""`
		TIndex   tindex.Service      `inject:""`
	}
)

func NewService() *Service {
	return new(Service)
}

// Write performs Write operation to a journal defined by tags.
func (s *Service) Write(ctx context.Context, tags string, lit model.Iterator) error {
	src, err := s.TIndex.GetOrCreateJournal(tags)
	if err != nil {
		return errors.Wrapf(err, "could not parse tags %s to turn it to journal source Id.", tags)
	}

	jrnl, err := s.Journals.GetOrCreate(ctx, src)
	if err != nil {
		return errors.Wrapf(err, "could not get or create new journal with src=%s by tags=%s", src, tags)
	}

	var iw iwrapper
	iw.pool = s.Pool
	iw.it = lit
	_, _, err = jrnl.Write(ctx, &iw)
	if err != nil {
		return errors.Wrapf(err, "could not write records to the journal %s by tags=%s", src, tags)
	}
	iw.close() // free resources
	return nil
}
