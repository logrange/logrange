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
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestNumOfDigits(t *testing.T) {
	assert.Equal(t, 1, NumOfDigits(-1))
	assert.Equal(t, 1, NumOfDigits(0))
	assert.Equal(t, 1, NumOfDigits(1))

	assert.Equal(t, 3, NumOfDigits(-123))
	assert.Equal(t, 3, NumOfDigits(123))

	assert.Equal(t, 19, NumOfDigits(math.MinInt64))
	assert.Equal(t, 19, NumOfDigits(math.MaxInt64))
}

func TestNumLexStr(t *testing.T) {
	assert.Equal(t, "0", NumLexStr(0, 1))
	assert.Equal(t, "1", NumLexStr(1, 1))
	assert.Equal(t, "-1", NumLexStr(-1, 1))

	assert.Equal(t, "01", NumLexStr(1, 2))
	assert.Equal(t, "10", NumLexStr(10, 2))
	assert.Equal(t, "-10", NumLexStr(-10, 2))

	assert.Equal(t, "001", NumLexStr(1, 3))
	assert.Equal(t, "010", NumLexStr(10, 3))

	assert.Equal(t, "100", NumLexStr(100, 3))
	assert.Equal(t, "-100", NumLexStr(-100, 3))

	assert.Equal(t, "0009223372036854775807",
		NumLexStr(math.MaxInt64, 22))
	assert.Equal(t, "-0009223372036854775808",
		NumLexStr(math.MinInt64, 22))
}
