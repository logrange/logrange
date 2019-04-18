package api

import (
	"context"
)

type (
	Client interface {
		Querier
		Ingestor
		Admin
		Pipes

		Close() error
	}
)

func Select(ctx context.Context, cli Client, qr *QueryRequest, streamMode bool,
	handler func(res *QueryResult)) error {

	res := &QueryResult{}
	limit := qr.Limit
	timeout := qr.WaitTimeout
	for ctx.Err() == nil && limit > 0 {
		qr.Limit = limit
		qr.WaitTimeout = timeout

		err := cli.Query(ctx, qr, res)
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
