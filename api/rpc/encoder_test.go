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
	"bytes"
	"github.com/logrange/logrange/api"
	bytes2 "github.com/logrange/range/pkg/utils/bytes"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"reflect"
	"testing"
)

func TestQueryRequest(t *testing.T) {
	testQueryRequest(t, &api.QueryRequest{})
	testQueryRequest(t, &api.QueryRequest{ReqId: 123412349182374, TagsCond: "a=b|c=d", Where: "adfasdfdsf", Pos: "ddd", Limit: 1234})
}

func TestQueryResult(t *testing.T) {
	testQueryResult(t, &api.QueryResult{})
	testQueryResult(t, &api.QueryResult{NextQueryRequest: api.QueryRequest{ReqId: 123412349182374, TagsCond: "a=b|c=d", Where: "adfasdfdsf", Pos: "ddd", Limit: 1234},
		Events: []*api.LogEvent{
			&api.LogEvent{1, "mes1", ""},
			&api.LogEvent{2, "mes2", "bbb=ttt"},
		}})
}

func TestQueryResultBuilder(t *testing.T) {
	qb := &queryResultBuilder{}
	qb.init(&bytes2.Pool{})
	qb.writeLogEvent(&api.LogEvent{1, "mes1", ""})
	qb.writeLogEvent(&api.LogEvent{2, "mes2", "aaa=bbb"})
	qb.writeQueryRequest(&api.QueryRequest{ReqId: 123412349182374, TagsCond: "a=b|c=d", Where: "adfasdfdsf", Pos: "ddd", Limit: 1234})
	var rqr api.QueryResult
	unmarshalQueryResult(qb.buf(), &rqr, true)
	qb.Close()

	tqr := &api.QueryResult{NextQueryRequest: api.QueryRequest{ReqId: 123412349182374, TagsCond: "a=b|c=d", Where: "adfasdfdsf", Pos: "ddd", Limit: 1234},
		Events: []*api.LogEvent{
			&api.LogEvent{1, "mes1", ""},
			&api.LogEvent{2, "mes2", "aaa=bbb"},
		}}
	if !reflect.DeepEqual(tqr, &rqr) {
		t.Fatal("tqr=", tqr.Events, ", must be equal to ", rqr)
	}
}

func testQueryRequest(t *testing.T, qr *api.QueryRequest) {
	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	n, err := writeQueryRequest(qr, ow)
	if err != nil || n != getQueryRequestSize(qr) || n != len(btb.Bytes()) {
		t.Fatal("err must be nil, and size n=", n, " must be ", getQueryRequestSize(qr), ", err=", err)
	}

	var rqr api.QueryRequest
	unmarshalQueryRequest(btb.Bytes(), &rqr, true)
	if !reflect.DeepEqual(&rqr, qr) {
		t.Fatal("qr=", qr, ", must be equal to ", rqr)
	}
}

func testQueryResult(t *testing.T, qr *api.QueryResult) {
	btb := &bytes.Buffer{}
	ow := &xbinary.ObjectsWriter{Writer: btb}
	n, err := writeQueryResult(qr, ow)
	if err != nil || n != getQueryResultSize(qr) || n != len(btb.Bytes()) {
		t.Fatal("err must be nil, and size n=", n, " must be ", getQueryResultSize(qr), ", err=", err)
	}

	var rqr api.QueryResult
	unmarshalQueryResult(btb.Bytes(), &rqr, true)
	if !reflect.DeepEqual(&rqr, qr) {
		t.Fatal("qr=", qr, ", must be equal to ", rqr)
	}
}
