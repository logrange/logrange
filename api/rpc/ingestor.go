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
	"github.com/logrange/logrange/pkg/journal"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
	"sync"
)

type (
	// ServerIngestor is a struct, which provides the ingestor RPC functionality.
	ServerIngestor struct {
		Journals *journal.Service `inject:""`
		MainCtx  context.Context  `inject:"mainCtx"`

		wg sync.WaitGroup
	}

	clntIngestor struct {
		rc rrpc.Client
	}

	writePacket struct {
		tags   string
		events []*api.LogEvent
	}

	// wpIterator is the struct which receives the slice of bytes and provides a records.Iteratro, sutable for
	// writing the data directly to a journal
	wpIterator struct {
		tags string
		lge  model.LogEvent
		buf  []byte
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
	err := wpi.init(reqBody)
	if err == nil {
		err = si.Journals.Write(si.MainCtx, wpi.tags, &wpi)
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

func (wpi *wpIterator) init(buf []byte) (err error) {
	wpi.buf = buf
	var idx int
	idx, wpi.tags, err = xbinary.UnmarshalString(buf, false)
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

// Next is a part of records.Iterator
func (wpi *wpIterator) Next(ctx context.Context) {
	wpi.read = false
}

// Get is a part of records.Iterator
func (wpi *wpIterator) Get(ctx context.Context) (model.LogEvent, tag.Line, error) {
	if wpi.read {
		return wpi.lge, tag.EmptyLine, nil
	}

	if wpi.cur >= wpi.recs {
		return wpi.lge, tag.EmptyLine, io.EOF
	}

	wpi.cur++

	var le api.LogEvent
	n, err := unmarshalLogEvent(wpi.buf[wpi.pos:], &le, false)
	if err != nil {
		return wpi.lge, tag.EmptyLine, io.EOF
	}
	wpi.pos += n
	wpi.read = true

	wpi.lge.Timestamp = le.Timestamp
	wpi.lge.Msg = le.Message

	return wpi.lge, tag.EmptyLine, nil
}

func (wpi *wpIterator) String() string {
	return fmt.Sprintf("buf len=%d, tags=%s, read=%t, pos=%d, recs=%d, cur=%d", len(wpi.buf), wpi.tags, wpi.read, wpi.pos, wpi.recs, wpi.cur)
}
