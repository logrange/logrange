package api

type (
	Client interface {
		Querier
		Ingestor
		Admin
		Streams

		Close() error
	}
)
