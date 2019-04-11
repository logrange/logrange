package api

import "context"

type (
	Client interface {
		Querier
		Ingestor
		Admin
		Streams

		Select(ctx context.Context, qr *QueryRequest, streamMode bool, handler func(res *QueryResult)) error
		Close() error
	}
)
