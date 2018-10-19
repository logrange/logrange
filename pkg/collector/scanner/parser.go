package scanner

import (
	"github.com/logrange/logrange/pkg/collector/model"
	"io"
)

type (
	// Parser provides an interface for retrieving records from a data-stream.
	// Implementations of the interface are supposed to be initialized by the
	// stream (io.Reader)
	Parser interface {
		io.Closer

		// NextRecord parses next record. It returns error if it could not parse
		// a record from the stream. io.EOF is returned if no new records found, but
		// end is reached.
		NextRecord() (*model.Record, error)

		// SetStreamPos specifies the stream position for the next record read
		SetStreamPos(pos int64) error

		// GetStreamPos returns position of the last successfully (error was nil)
		// returned record by nextRecord(). If nextRecord() returned non-nil
		// error the getStreamPos() returned value is not relevant and should
		// not be used as a valid stream position.
		GetStreamPos() int64

		// GetStat returns the parser statistic
		GetStats() *ParserStats
	}

	// ParserStat struct contains information about the parser statistics
	ParserStats struct {
		DataType string
		Size     int64
		Pos      int64
		// for text parsers provides information about found date-time formats
		DateFormats map[string]int64
	}
)
