// Copyright 2018-2019 The logrange Authors
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

package strutil

import (
	"fmt"
	"strings"
)

// Counts number of digits in the given number n
// 		Examples:
// 			NumOfDigits(0) returns 1
// 			NumOfDigits(-1) returns 1
// 			NumOfDigits(100) returns 3
func NumOfDigits(n int) int {
	if (n >= 0 && n < 10) || (n < 0 && n > -10) {
		return 1
	}
	d := 0
	for n > 0 || n < 0 {
		n /= 10
		d++
	}
	return d
}

// Returns lexicographical representation of the given
// number n with respect to max (digits) length d
// 		Examples:
//			NumLexStr(1, 3) returns 001
//			NumLexStr(12, 3) returns 012
//			NumLexStr(-1, 3) returns -001
func NumLexStr(n int, maxDigits int) string {
	nDigits := NumOfDigits(n)
	if maxDigits < nDigits {
		panic("util.NumLexStr: maxDigits < nDigits")
	}

	var (
		s string
		k uint
	)

	if n >= 0 {
		s = ""
		k = uint(n)
	} else {
		s = "-"
		k = uint(-n)
	}
	return fmt.Sprintf("%s%s%d", s,
		strings.Repeat("0", maxDigits-nDigits), k)
}
