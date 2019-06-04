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

// Select is a helper function which allows to perform group requests to
// the server. It expects the following params:
//   ctx  - context for the call. If the context is closed, the function execution
// 			will be interrupted.
// 	 querier 	- the Querier implementation.
// 	 qr	- the query request. It should contain number of records to be read. In
// 		stream mode, the value is used for batch reading.
//   streamMode - the flag indicates whether the read should be in stream mode or not
// 	 handler - the function to serve the record result. It must not be nil
//
// In stream mode records will be read until the read is interrupted. So the result
// is not limited at all. In stream mode qr.Limit is used as a parameter for reading
// the number of records in one batch. So the value should not be big (hundreds or
// thousands records). When the end of result stream is reached, the function will
// not be over, but it continues to read records using qr.WaitTimeout value for
// waiting new records. In stream mode the function is over only when context is
// closed or an error happens while the query is executed.
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
