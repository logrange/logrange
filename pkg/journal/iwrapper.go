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
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/utils/bytes"
)

type (
	// iwrapper struct wraps LogEvents iterator and provides records.Iterator interface
	iwrapper struct {
		it   model.Iterator
		pool *bytes.Pool
		rec  []byte
		read bool

		minTs, maxTs uint64
	}
)

func (iw *iwrapper) Next(ctx context.Context) {
	iw.read = false
	iw.it.Next(ctx)
}

func (iw *iwrapper) Get(ctx context.Context) (records.Record, error) {
	if iw.read {
		return iw.rec, nil
	}

	lge, _, err := iw.it.Get(ctx)
	if err != nil {
		return nil, err
	}

	if iw.minTs == 0 || iw.minTs > lge.Timestamp {
		iw.minTs = lge.Timestamp
	}

	if iw.maxTs == 0 || iw.maxTs < lge.Timestamp {
		iw.maxTs = lge.Timestamp
	}

	sz := lge.WritableSize()
	if cap(iw.rec) < sz {
		iw.pool.Release(iw.rec)
		iw.rec = iw.pool.Arrange(sz)
	}
	iw.rec = iw.rec[:sz]
	lge.Marshal(iw.rec)

	return iw.rec, nil

}

func (iw *iwrapper) close() {
	if iw.rec != nil {
		iw.pool.Release(iw.rec)
		iw.rec = nil
	}
}
