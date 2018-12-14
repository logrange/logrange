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

package ctrlr

import (
	"context"
	"sync"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records/journal"
)

type (
	jrnlController struct {
		logger log4g.Logger
		lock   sync.Mutex
	}
)

func NewJournalController() journal.Controller {
	jc := new(jrnlController)
	jc.logger = log4g.GetLogger("journal.Controller")
	return jc
}

// GetOrCreate returns journal by its name. It is part of journal.Contorller
func (jc *jrnlController) GetOrCreate(ctx context.Context, jname string) (journal.Journal, error) {
	return nil, nil
}
