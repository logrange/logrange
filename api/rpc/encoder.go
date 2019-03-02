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
	"github.com/logrange/logrange/api"
	"github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
)

// api.LogEvent

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

func unmarshalLogEvent(buf []byte, res *api.LogEvent, newBuf bool) (int, error) {
	n, v, err := xbinary.UnmarshalUint64(buf)
	if err != nil {
		return 0, err
	}
	nn := n
	res.Timestamp = v

	n, msg, err := xbinary.UnmarshalString(buf[nn:], newBuf)
	nn += n
	if err != nil {
		return nn, err
	}
	res.Message = msg

	n, tags, err := xbinary.UnmarshalString(buf[nn:], newBuf)
	nn += n
	res.Tags = tags
	return nn, err
}

// api.QueryRequest
func writeQueryRequest(qr *api.QueryRequest, ow *xbinary.ObjectsWriter) (int, error) {
	n, err := ow.WriteUint64(uint64(qr.ReqId))
	if err != nil {
		return n, err
	}
	nn := n

	n, err = ow.WriteString(qr.Query)
	nn += n
	if err != nil {
		return nn, err
	}

	n, err = ow.WriteString(qr.Pos)
	nn += n
	if err != nil {
		return nn, err
	}

	n, err = ow.WriteUint32(uint32(qr.Limit))
	nn += n
	return nn, err
}

func getQueryRequestSize(qr *api.QueryRequest) int {
	return xbinary.WritableStringSize(qr.Query) +
		xbinary.WritableStringSize(qr.Pos) + 12
}

func unmarshalQueryRequest(buf []byte, qr *api.QueryRequest, newBuf bool) (int, error) {
	n, v, err := xbinary.UnmarshalUint64(buf)
	if err != nil {
		return 0, err
	}
	nn := n
	qr.ReqId = v

	n, s, err := xbinary.UnmarshalString(buf[nn:], newBuf)
	nn += n
	if err != nil {
		return nn, err
	}
	qr.Query = s

	n, s, err = xbinary.UnmarshalString(buf[nn:], newBuf)
	nn += n
	if err != nil {
		return nn, err
	}
	qr.Pos = s

	n, i, err := xbinary.UnmarshalUint32(buf[nn:])
	nn += n
	qr.Limit = int(i)
	return nn, err
}

// api.QueryResult
type queryResultBuilder struct {
	wrtr bytes.Writer
	ow   xbinary.ObjectsWriter
	recs int
}

func (qr *queryResultBuilder) init(p *bytes.Pool) {
	qr.wrtr.Reset(4096, p)
	qr.ow.Writer = &qr.wrtr
	qr.recs = 0
	qr.ow.WriteUint32(0) // reserve for log event size
}

func (qr *queryResultBuilder) writeLogEvent(le *api.LogEvent) error {
	_, err := writeLogEvent(le, &qr.ow)
	qr.recs++
	return err
}

func (qr *queryResultBuilder) writeQueryRequest(qryReq *api.QueryRequest) error {
	xbinary.MarshalUint32(uint32(qr.recs), qr.wrtr.Buf())
	_, err := writeQueryRequest(qryReq, &qr.ow)
	return err
}

func (qr *queryResultBuilder) buf() []byte {
	return qr.wrtr.Buf()
}

func (qr *queryResultBuilder) Close() error {
	return qr.wrtr.Close()
}

func writeQueryResult(qr *api.QueryResult, ow *xbinary.ObjectsWriter) (int, error) {
	n, err := ow.WriteUint32(uint32(len(qr.Events)))
	nn := n
	for _, ev := range qr.Events {
		n, err = writeLogEvent(ev, ow)
		nn += n
		if err != nil {
			return nn, err
		}
	}

	n, err = writeQueryRequest(&qr.NextQueryRequest, ow)
	nn += n
	return nn, nil
}

func getQueryResultSize(qr *api.QueryResult) int {
	res := getQueryRequestSize(&qr.NextQueryRequest)
	// the slice size goes as well
	res += 4
	for _, ev := range qr.Events {
		res += getLogEventSize(ev)
	}
	return res
}

func unmarshalQueryResult(buf []byte, res *api.QueryResult, newBuf bool) (int, error) {
	n, ln, err := xbinary.UnmarshalUint32(buf)
	nn := n
	if err != nil {
		return nn, err
	}
	if ln > 0 {
		res.Events = make([]*api.LogEvent, int(ln))
	} else {
		res.Events = nil
	}

	for i := 0; i < int(ln); i++ {
		le := new(api.LogEvent)
		n, err = unmarshalLogEvent(buf[nn:], le, newBuf)
		nn += n
		if err != nil {
			return nn, err
		}
		res.Events[i] = le
	}

	n, err = unmarshalQueryRequest(buf[nn:], &res.NextQueryRequest, newBuf)
	nn += n
	return nn, err
}
