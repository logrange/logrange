// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this f except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"context"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestPureParseAndReposition(t *testing.T) {
	fn := absPath("../testdata/logs/ubuntu/var/log/mongodb/mongodb.log")
	pp, err := NewPureParser(fn, 4096)
	assert.NoError(t, err)

	pos := make([]int64, 0)
	rec := make([]*model.Record, 0)
	now := time.Now()
	for i := 0; i < 10; i += 2 {
		pos = append(pos, pp.GetStreamPos())
		r, err := pp.NextRecord(context.Background())
		assert.True(t, r.Date.Sub(now) < time.Second, "expecting small difference, but ", r.Date.Sub(now))
		assert.NoError(t, err)
		assert.NotNil(t, r)
		rec = append(rec, r)
	}

	for i := 0; i < len(pos); i++ { // shuffle a bit
		rand.Seed(time.Now().UnixNano())
		ri := rand.Intn(len(pos))
		pos[i], rec[i] = pos[ri], rec[ri]
	}

	for i, exp := range rec {
		err = pp.SetStreamPos(pos[i])
		assert.NoError(t, err)
		act, err := pp.NextRecord(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, act)
		assert.Equal(t, exp.Data, act.Data)
	}

	err = pp.Close()
	assert.NoError(t, err)
}
