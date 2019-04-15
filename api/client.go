package api

type (
	Client interface {
		Querier
		Ingestor
		Admin
		Pipes

		Close() error
	}
)
