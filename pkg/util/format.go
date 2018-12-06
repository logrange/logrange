// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package util

import (
	"fmt"
	"strings"

	"github.com/jrivets/gorivets"
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
