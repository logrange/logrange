package shell

import (
	"github.com/logrange/logrange/api"
	"testing"
	"time"
)

func BenchmarkPrintResult(b *testing.B) {
	qr := api.QueryResult{Events: []*api.LogEvent{
		{
			Timestamp: time.Now().UnixNano(),
			Message:   "BenchmarkPrintResult",
			Tags:      "t1=v2,t2=v2",
		},
	}}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		printResults(&qr, defaultEvFmtTemplate, &nilWriter{})
	}
}

type nilWriter struct{}

func (*nilWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}
