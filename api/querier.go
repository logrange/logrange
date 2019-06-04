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

package api

import (
	"context"
	"fmt"
)

type (

	// Querier interface allows to perform read operations.
	Querier interface {
		// Query runs lql to collect the server data and returns it in the QueryResult.
		// It returns an error which indicates that the query could not be delivered to
		// the server. If there is no communication problems, but server could not perform
		// the query by any reason, the function will return nil, but the server error could
		// be found in res.Err
		//
		// The Query expects res param, where the Query result will be placed (see QueryResult).
		// The QueryResult contains the data, which is read and the NextQueryRequest field
		// which value may be used for consecutive read requests. Using res.NextQueryRequest allows
		// to improve the read performance. Also the res.NextQueryRequest.Pos contains the next
		// record read position, which could be used for iterating over the result record collection.
		Query(ctx context.Context, req *QueryRequest, res *QueryResult) error
	}

	// QueryRequest struct describes a request for reading records
	QueryRequest struct {
		// ReqId identifies the request Id on server side. The field should not be populated by client in
		// its first request, but it can be taken from QueryResult.NextQueryRequest for consecutive
		// requests.
		//
		// The field is helpful when records should be read in order by several consecutive reaquests.
		// Server could cache some resources to perform the iteration quickly. So the server could return
		// a value in QueryResult.NextQueryRequest field, which could be provided with the following request.
		ReqId uint64

		// Query contains SELECT statement for reading records. This field contains the request, but
		// some parameters like POSTION, OFFSET and LIMIT could be overwritten by fields from the QueryResult
		Query string

		// Pos contains the next read record position. For consecutive batch reading the value
		// can be taken from the previous request result QueryResult.NextQueryRequest
		Pos string

		// WaitTimeout in seconds provide waiting new data timeout in case of the request starts from
		// the end of result stream (no records). The timout cannot exceed 60 seconds. When the timeout
		// expires and no data is arrived response with no data will be returned.
		WaitTimeout int

		// Offset contains the offset from the current position (either positive or negative). For
		// batch read it should be set to 0 for non-first request.
		Offset int

		// Limit defines the maximum number of records which could be read from the sources
		Limit int
	}

	// QeryResult struct contains the result returned by the server in a response on LQL execution (see Querier.Query)
	QueryResult struct {
		// Events slice contains the result of the query execution
		Events []*LogEvent

		// NextQueryRequest contains the query for reading next porition of events. It makes sense only if Err is
		// nil.
		NextQueryRequest QueryRequest

		// Err the operation error. If the Err is nil, the operation successfully executed
		Err error `json:"-"`
	}
)

// String is part of Stringify interface
func (qr *QueryRequest) String() string {
	return fmt.Sprintf("{ReqId: %d, Query: %s, Pos: %s, WaitTimeout: %d, Offset: %d, Limit: %d}", qr.ReqId, qr.Query, qr.Pos, qr.WaitTimeout, qr.Offset, qr.Limit)
}

// String is part of Stringify interface
func (qres *QueryResult) String() string {
	return fmt.Sprintf("{Events: %d, NextQueryReq: %s, Err: %v}", len(qres.Events), qres.NextQueryRequest.String(), qres.Err)
}
