package journal

import (
	"testing"
)

func BenchmarkJidFromName(b *testing.B) {
	str := "some name for a journal"
	for i := 0; i < b.N; i++ {
		JidFromName(str)
	}
}
