package api

import (
	"context"
)

type (

	// Client interface provides all api functionality in one interface.
	Client interface {
		Querier
		Ingestor
		Admin
		Pipes

		// Close allows to close connection to the server and to free all resources
		Close() error
	}
)

// Select is a helper function which allows to perform one or many requests to
// the server. The function supports 2 modes - limited select and stream mode, that
// are controlled by streamMode flag
//
// The limited select is used when streamMode=false. In this mode Select will
// return requested number of records, or all records whatever is less. The limit must
// be explicitly set in qr.Limit param, which will overwrite value specified in qr.Query
//
// In stream mode records will be read until the Select is interrupted. So the number
// of records in the result is not limited at all. In stream mode qr.Limit is used
// as a parameter for reading the number of records in one batch request. So the value
// should not be big (hundreds or thousands records). When the end of result stream
// is reached, the function will not be over, but it continues to read records using
// qr.WaitTimeout value for waiting the new records. In stream mode the function
// is over only when context is closed or an error happens while the query is executed.
//
// The results of read operations will be passed to the handler, a call-back function.
// Select will call handler any time when it could make a query to the server and there
// is no communication errors
//
// The function returns error if the query was unsuccessful by any reason.
//
func Select(ctx context.Context, querirer Querier, qr *QueryRequest, streamMode bool,
	handler func(res *QueryResult)) error {

	res := &QueryResult{}
	limit := qr.Limit
	timeout := qr.WaitTimeout
	for ctx.Err() == nil && limit > 0 {
		qr.Limit = limit
		qr.WaitTimeout = timeout

		err := querirer.Query(ctx, qr, res)
		if err != nil {
			return err
		}

		if len(res.Events) != 0 {
			handler(res)
		}

		qr = &res.NextQueryRequest
		if !streamMode {
			limit -= len(res.Events)
			if len(res.Events) == 0 {
				break
			}
		}
	}

	return nil
}
