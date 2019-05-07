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

package date

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseFormat1(t *testing.T) {
	//fmt1
	p := NewDefaultParser()
	str := "test prefix Tue Jan 30 00:42:28.694 <kernel> Setting BTCoex " +
		"Config: enable_2G:1, profile_2g:0, enable_5G:1, profile_5G:0"
	exp := time.Date(time.Now().Year(), 1, 30, 0, 42, 28, 694000000, time.UTC)
	act, fmt := p.Parse([]byte(str))
	assert.NotNil(t, fmt)
	assert.Equal(t, exp, act)

	//fmt2
	locTm, err := time.Parse("-0700", "+0000")
	assert.NoError(t, err)
	exp = time.Date(2017, 12, 25, 21, 57, 12, 934000000, locTm.Location())
	str = "2017-12-25T21:57:12.934+0000 [clientcursormon]  connections:0"
	act, fmt = p.Parse([]byte(str))
	assert.NotNil(t, fmt)
	assert.Equal(t, exp, act)

	//fmt3
	locTm, err = time.Parse("-0700", "+0800")
	assert.NoError(t, err)
	exp = time.Date(1971, 3, 25, 6, 20, 45, 0, locTm.Location())
	str = "69.164.145.164 - - [25/Mar/1971:06:20:45 +0800] \"GET /?utm_source=Trivia-Revcontent&" +
		"utm_medium=editorial_news&utm_term=5933&utm_content=2629075&utm_campaign=354893 HTTP/1.1\" 200 612 \"-\" \"-\""
	act, fmt = p.Parse([]byte(str))
	assert.NotNil(t, fmt)
	assert.Equal(t, exp, act)

	//fmt4 (no datetime)
	exp = time.Time{}
	act, fmt = p.Parse([]byte("no datetime"))
	assert.Nil(t, fmt)
	assert.Equal(t, exp, act)
}

func TestCustomParse(t *testing.T) {
	p := NewParser("hh:mm:ss")
	tm, f := p.Parse([]byte("12:43:54"))
	if f == nil {
		t.Fatal("format must not be nil")
	}
	fmt.Println(tm.String())
}
