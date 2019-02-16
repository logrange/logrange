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

package rpc

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/logevent"
	"github.com/logrange/range/pkg/records"
	"github.com/logrange/range/pkg/records/journal"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
)

type (
	// ServerIngestor is a struct, which provides the ingestor RPC functionality.
	ServerIngestor struct {
		Pool         *bytes.Pool        `inject:""`
		TagIdGenSize int                `inject:"TagIdGenSize,optional:10000"`
		JrnlCtrlr    journal.Controller `inject:""`

		tig *logevent.TagIdGenerator
	}

	clntIngestor struct {
		rc rrpc.Client
	}

	writePacket struct {
		src    string
		tags   string
		events []*api.LogEvent
	}

	// wpIterator is the struct which receives the slice of bytes and provides a xbinary.WIterator, sutable for
	// writing the data directly to a journal
	wpIterator struct {
		pool   *bytes.Pool
		tig    *logevent.TagIdGenerator
		src    string
		tags   string
		tagsId uint64
		buf    []byte
		rec    []byte
		read   bool
		pos    int
		recs   int
		cur    int
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
	if si.TagIdGenSize < 100 {
		return fmt.Errorf("Too small Tag Id Generator buffer size=%d", si.TagIdGenSize)
	}
	si.tig = logevent.NewTagIdGenerator(si.TagIdGenSize)

	return nil
}

func (ci *clntIngestor) Write(src, tags string, evs []*api.LogEvent) error {
	var wp writePacket
	wp.src = src
	wp.tags = tags
	wp.events = evs

	_, errOp, err := ci.rc.Call(context.Background(), cRpcEpIngestorWrite, &wp)
	if errOp != nil {
		err = errOp
	}

	return err
}

func (si *ServerIngestor) ingestorWrite(reqId int32, reqBody []byte, sc *rrpc.ServerConn) {
	var wpi wpIterator
	wpi.pool = si.Pool
	wpi.tig = si.tig
	err := wpi.init(reqBody)
	if err == nil {
		var jrnl journal.Journal
		jrnl, err = si.JrnlCtrlr.GetOrCreate(context.Background(), wpi.src)
		if err == nil {
			_, _, err = jrnl.Write(context.Background(), &wpi)
		}
	}

	sc.SendResponse(reqId, err, cEmptyResponse)
}

// EncodedSize part of rrpc.Encoder interface
func (wp *writePacket) WritableSize() int {
	res := xbinary.WritableStringSize(wp.src) + xbinary.WritableStringSize(wp.tags)
	// array size goes as well
	res += 4
	for _, ev := range wp.events {
		res += getLogEventSize(ev)
	}
	return res
}

// Encode part of rrpc.Encoder interface
func (wp *writePacket) WriteTo(ow *xbinary.ObjectsWriter) (int, error) {
	n, err := ow.WriteString(wp.src)
	if err != nil {
		return n, err
	}
	nn := n

	n, err = ow.WriteString(wp.tags)
	nn += n
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

func writeLogEvent(ev *api.LogEvent, ow *xbinary.ObjectsWriter) (int, error) {
	n, err := ow.WriteUint64(uint64(ev.Timestamp))
	if err != nil {
		return n, err
	}
	nn := n

	n, err = ow.WriteString(ev.Message)
	nn += n
	if err != nil {
		return nn, err
	}

	n, err = ow.WriteString(ev.Tags)
	nn += n
	return nn, err
}

func getLogEventSize(ev *api.LogEvent) int {
	return xbinary.WritableStringSize(ev.Message) + xbinary.WritableStringSize(ev.Tags) + 8
}

func unmarshalLogEvent(res *api.LogEvent, buf []byte) (int, error) {
	n, v, err := xbinary.UnmarshalInt64(buf)
	if err != nil {
		return 0, err
	}
	nn := n
	res.Timestamp = v

	n, msg, err := xbinary.UnmarshalString(buf[nn:], false)
	nn += n
	if err != nil {
		return nn, err
	}
	res.Message = msg

	n, tags, err := xbinary.UnmarshalString(buf[nn:], false)
	nn += n
	res.Tags = tags
	return nn, err
}

func (wpi *wpIterator) init(buf []byte) (err error) {
	wpi.buf = buf
	idx := 0
	idx, wpi.src, err = xbinary.UnmarshalString(buf, false)
	if err != nil {
		return
	}

	idx2 := 0
	idx2, wpi.tags, err = xbinary.UnmarshalString(buf[idx:], false)
	if err != nil {
		return
	}
	_, err = logevent.CheckTags(wpi.tags)
	if err != nil {
		return
	}

	idx += idx2
	ln := uint32(0)
	idx2, ln, err = xbinary.UnmarshalUint32(buf[idx:])
	if err != nil {
		return err
	}
	wpi.recs = int(ln)
	wpi.pos = idx + idx2
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
	n, err := unmarshalLogEvent(&le, wpi.buf[wpi.pos:])
	if err != nil {
		return nil, err
	}
	wpi.pos += n
	wpi.read = true

	var lge logevent.LogEvent
	lge.Timestamp = le.Timestamp
	lge.Msg = le.Message
	lge.Tags = le.Tags
	if len(lge.Tags) == 0 && len(wpi.tags) > 0 {
		if wpi.tagsId == 0 {
			lge.Tags = wpi.tags
			wpi.tagsId = wpi.tig.GetOrCreateId(wpi.tags)
		}
		lge.TgId = int64(wpi.tagsId)
	}

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
	return fmt.Sprintf("buf len=%d, src=%s, tags=%s, read=%t, pos=%d, recs=%d, cur=%d", len(wpi.buf), wpi.src, wpi.tags, wpi.read, wpi.pos, wpi.recs, wpi.cur)
}
