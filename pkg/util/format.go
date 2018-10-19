package util

import (
	"fmt"
	"github.com/jrivets/gorivets"
	"strings"
)

// FormatSize prints the size by scale 1000, ex: 23Kb(23450)
func FormatSize(val int64) string {
	if val < 1000 {
		return fmt.Sprint(val)
	}
	return fmt.Sprint(gorivets.FormatInt64(val, 1000), "(", val, ")")
}

func FormatProgress(size int, perc float64) string {
	fl := int(float64(size-2) * perc / 100.0)

	if fl > (size - 2) {
		fl = size - 2
	}

	if fl < 0 {
		fl = 0
	}

	empt := size - 2 - fl
	return fmt.Sprintf("%5.2f%% |%s%s|", perc, strings.Repeat("#", fl), strings.Repeat("-", empt))
}
