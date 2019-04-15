package api

import "context"

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

	limit := qr.Limit
	timeout := qr.WaitTimeout
	for ctx.Err() == nil {
		qr.Limit = limit
		qr.WaitTimeout = timeout

		res := &QueryResult{}
		err := cli.Query(ctx, qr, res)
		if err != nil {
			return err
		}

		if len(res.Events) != 0 {
			handler(res)
		}

		qr = &res.NextQueryRequest
		if !streamMode {
			if limit <= 0 || len(res.Events) == 0 {
				break
			}
			limit -= len(res.Events)
		}
	}

	return nil
}
