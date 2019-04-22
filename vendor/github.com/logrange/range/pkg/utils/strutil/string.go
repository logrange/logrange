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
	"bytes"
	"crypto/rand"
)

func GetRandomString(size int, abc string) string {
	var buffer bytes.Buffer
	var val [64]byte
	var buf []byte

	if size > len(val) {
		buf = make([]byte, size)
	} else {
		buf = val[:size]
	}

	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}

	for _, v := range buf {
		buffer.WriteString(string(abc[int(v)%len(abc)]))
	}

	return buffer.String()
}

// Bytes2String transforms bitstream to a string. val contains bytes and
// only lower bits from any value is used for the calculation. abet - is an
// alpabet which is used for forming the result string.
func Bytes2String(val []byte, abet string, bits int) string {
	kap := len(val) * 8 / bits
	abl := len(abet)
	res := make([]byte, 0, kap)
	mask := (1 << uint(bits)) - 1
	i := 0
	shft := 0
	for i < len(val) {
		b := int(val[i]) >> uint(shft)
		bSize := 8 - shft
		if bSize <= bits {
			i++
			if i < (len(val)) {
				shft = bits - bSize
				b |= int(val[i]) << uint(bSize)
			}
		} else {
			shft += bits
		}
		res = append(res, abet[(b&mask)%abl])
	}
	return string(res)
}

// RemoveDups returns a slice where every element from ss meets only once
func RemoveDups(ss []string) []string {
	j := 0
	found := map[string]bool{}
	for i, s := range ss {
		if !found[s] {
			found[s] = true
			ss[j] = ss[i]
			j++
		}
	}
	return ss[:j]
}

// Swaps (in place) odd and even positions of the given string slice,
// can be useful in reversing 'map' when represented as slice of {k1,v1,k2,v2}
// 		Examples:
//			SwapEvenOdd([]string{1}) returns []string{1}
//			SwapEvenOdd([]string{1,2,3}) returns []string{2,1,3}
//			SwapEvenOdd([]string{1,2,3,4}) returns []string{2,1,4,3}
func SwapEvenOdd(ss []string) []string {
	for i := 0; i < len(ss)-1; i++ {
		if i%2 == 0 {
			ss[i], ss[i+1] = ss[i+1], ss[i]
		}
	}
	return ss
}
