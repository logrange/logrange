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

package rpc

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/tindex"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/journal"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
	"sync"
)

type (
	// ServerIngestor is a struct, which provides the ingestor RPC functionality.
	ServerIngestor struct {
		Pool      *bytes.Pool        `inject:""`
		JrnlCtrlr journal.Controller `inject:""`
		TIndex    tindex.Service     `inject:""`
		MainCtx   context.Context    `inject:"mainCtx"`

		wg sync.WaitGroup
	}

	clntIngestor struct {
		rc rrpc.Client
	}

	writePacket struct {
		tags   string
		events []*api.LogEvent
	}

	// wpIterator is the struct which receives the slice of bytes and provides a xbinary.WIterator, sutable for
	// writing the data directly to a journal
	wpIterator struct {
		pool *bytes.Pool
		src  string
		buf  []byte
		rec  []byte
		read bool
		pos  int
		recs int
		cur  int
	}
)

type emptyResponse int

func (er emptyResponse) WritableSize() int {
	return 0
}

func (er emptyResponse) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	return 0, nil
}

const cEmptyResponse = emptyResponse(0)

func NewServerIngestor() *ServerIngestor {
	return new(ServerIngestor)
}

func (si *ServerIngestor) Init(ctx context.Context) error {
	return nil
}

func (si *ServerIngestor) Shutdown() {
	si.wg.Wait()
}

func (ci *clntIngestor) Write(ctx context.Context, tags string, evs []*api.LogEvent, res *api.WriteResult) error {
	var wp writePacket
	wp.tags = tags
	wp.events = evs

	buf, errOp, err := ci.rc.Call(ctx, cRpcEpIngestorWrite, &wp)
	if res != nil {
		res.Err = errOp
	}
	ci.rc.Collect(buf)

	return err
}

func (si *ServerIngestor) write(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	si.wg.Add(1)
	defer si.wg.Done()

	var wpi wpIterator
	wpi.pool = si.Pool
	err := wpi.init(reqBody, si.TIndex)
	if err == nil {
		var jrnl journal.Journal
		jrnl, err = si.JrnlCtrlr.GetOrCreate(si.MainCtx, wpi.src)
		if err == nil {
			_, _, err = jrnl.Write(si.MainCtx, &wpi)
		}
	}

	sc.SendResponse(reqId, err, cEmptyResponse)
}

// EncodedSize part of rrpc.Encoder interface
func (wp *writePacket) WritableSize() int {
	res := xbinary.WritableStringSize(wp.tags)
	// array size goes as well
	res += 4
	for _, ev := range wp.events {
		res += getLogEventSize(ev)
	}
	return res
}

// Encode part of rrpc.Encoder interface
func (wp *writePacket) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	n, err := ow.WriteString(wp.tags)
	nn := n
	if err != nil {
		return nn, err
	}

	n, err = ow.WriteUint32(uint32(len(wp.events)))
	nn += n
	for _, ev := range wp.events {
		n, err = writeLogEvent(ev, ow)
		nn += n
		if err != nil {
			return nn, err
		}
	}

	return nn, nil
}

func (wpi *wpIterator) init(buf []byte, tidx tindex.Service) (err error) {
	wpi.buf = buf
	idx, tags, err := xbinary.UnmarshalString(buf, false)
	if err != nil {
		return err
	}

	wpi.src, err = tidx.GetOrCreateJournal(tags)
	if err != nil {
		return err
	}

	ln := uint32(0)
	var n int
	n, ln, err = xbinary.UnmarshalUint32(buf[idx:])
	if err != nil {
		return err
	}

	wpi.recs = int(ln)
	wpi.pos = idx + n
	wpi.cur = 0
	wpi.read = false

	return
}

// Next is a part of xbinary.WIterator
func (wpi *wpIterator) Next(ctx context.Context) {
	wpi.read = false
}

// Get is a part of xbinary.WIterator
func (wpi *wpIterator) Get(ctx context.Context) (records.Record, error) {
	if wpi.read {
		return wpi.rec, nil
	}

	if wpi.cur >= wpi.recs {
		return nil, io.EOF
	}

	wpi.cur++

	var le api.LogEvent
	n, err := unmarshalLogEvent(wpi.buf[wpi.pos:], &le, false)
	if err != nil {
		return nil, err
	}
	wpi.pos += n
	wpi.read = true

	var lge model.LogEvent
	lge.Timestamp = le.Timestamp
	lge.Msg = le.Message

	sz := lge.WritableSize()
	if cap(wpi.rec) < sz {
		wpi.pool.Release(wpi.rec)
		wpi.rec = wpi.pool.Arrange(sz)
	}
	wpi.rec = wpi.rec[:sz]
	lge.Marshal(wpi.rec)

	return wpi.rec, nil
}

func (wpi *wpIterator) String() string {
	return fmt.Sprintf("buf len=%d, src=%s, read=%t, pos=%d, recs=%d, cur=%d", len(wpi.buf), wpi.src, wpi.read, wpi.pos, wpi.recs, wpi.cur)
}
