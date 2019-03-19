package api

type (
	Client interface {
		Querier
		Ingestor
		Admin

		Close() error
	}
)
