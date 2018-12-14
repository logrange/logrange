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
	"testing"

	"github.com/logrange/logrange/pkg/cluster/model"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type jrnlCatalogTest struct {
}

func (jct *jrnlCatalogTest) GetJournalInfo(ctx context.Context, jname string) (model.JournalInfo, error) {
	return model.JournalInfo{}, nil
}

func (jct *jrnlCatalogTest) ReportLocalChunks(ctx context.Context, jname string, chunks []chunk.Id) error {
	return nil
}

func TestAdvertiserAdvertise(t *testing.T) {
	a := newAdvertiser(&jrnlCatalogTest{})
	a.advertise("test", []chunk.Id{1})
}
