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
	"github.com/logrange/logrange/api"
	rrpc "github.com/logrange/range/pkg/rpc"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"io"
)

type (
	// ServerIngestor is a struct, which provides the ingestor RPC functionality.
	ServerIngestor struct {
	}

	clntIngestor struct {
		rc rrpc.Client
	}

	writePacket struct {
		src    string
		tags   string
		events []*api.LogEvent
	}

	// wpIterator is the struct which receives the slice of bytes and provides a records.Iterator, sutable for
	// writing the data directly to a journal
	wpIterator struct {
		src  string
		tags string
		buf  []byte
		recs int
		cur  int
	}
)

func NewServerIngerstor() *ServerIngestor {
	return new(ServerIngestor)
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

}

// EncodedSize part of rrpc.Encoder interface
func (wp *writePacket) EncodedSize() int {
	res := xbinary.SizeString(wp.src) + xbinary.SizeString(wp.tags)
	// array size goes as well
	res += 4
	for _, ev := range wp.events {
		res += getLogEventSize(ev)
	}
	return res
}

// Encode part of rrpc.Encoder interface
func (wp *writePacket) Encode(w io.Writer) error {
	_, err := xbinary.WriteString(wp.src, w)
	if err != nil {
		return err
	}

	_, err = xbinary.WriteString(wp.tags, w)
	if err != nil {
		return err
	}

	_, err = xbinary.WriteUint32(uint32(len(wp.events)), w)
	for _, ev := range wp.events {
		err = writeLogEvent(ev, w)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeLogEvent(ev *api.LogEvent, w io.Writer) error {
	_, err := xbinary.WriteInt64(ev.Timestamp, w)
	if err != nil {
		return err
	}

	_, err = xbinary.WriteString(ev.Message, w)
	if err != nil {
		return err
	}

	_, err = xbinary.WriteString(ev.Tags, w)
	return err
}

func getLogEventSize(ev *api.LogEvent) int {
	return xbinary.SizeString(ev.Message) + xbinary.SizeString(ev.Tags) + 8
}

func (wpi *wpIterator) init(buf []byte) (err error) {
	wpi.buf = buf
	idx := 0
	idx, wpi.src, err = xbinary.UnmarshalString(buf, false)
	if err != nil {
		return
	}

	idx, wpi.tags, err = xbinary.UnmarshalString(buf[idx:], false)
	if err != nil {
		return
	}

	//ln := uint32(0)
	//idx, ln, err = xbinary.UnmarshalUint32(buf[idx:])
	//if err != nil {
	//	return
	//}

	return
}
