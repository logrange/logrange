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
	"strings"
)

type (
	escaper interface {
		Escape(s string) string
		Unescape(s string) string
	}

	// It is safe for concurrent use by multiple goroutines
	StringEscaper struct {
		e         escaper
		escaper   *strings.Replacer
		unescaper *strings.Replacer
	}
)

// FileName escaper is intended to sanitize filenames,
// i.e. to escape a file name, so that it doesn't contain
// any 'special' symbols which could be interpreted like commands (e.g. by shell)
// or are not allowed in the file names.
// The escaper is safe to be used simultaneously by multiple goroutines.
//
// WARNING: For backward compatibility, it is very important to keep
// the same code leader/prefix and the same order of escapeTerms,
// since the order affects on how we generate the code for every term.
// Don't remove the codes from here and if you need to add a new one add it to the end.
//
var FileNameEscaper = NewStringEscaper("_",
	"/", "\\", "`", "*", "|", ";", "\"", "'", ":")

// Returns escaper which generates escape code for every given term.
// Using the returned escaper one can escape/unescape strings in accordance with
// the generated (code,term) table.
//
// NOTE: Auto generates codes for escaping, order is important (!),
// i.e. depending on position in escapeTerms we generate escape code,
// so if order changes we won't be able to unescape correctly, previous escaping...
// To avoid one escape code to be a prefix of another the codes are numbers
// in format safe for lexicographical comparison, i.e. 00, 01, 02, ..., 10, etc.
func NewStringEscaper(leader string, escapeTerms ...string) *StringEscaper {
	escapeTerms = RemoveDups(append([]string{leader}, escapeTerms...)) // prepend leader && remove dups
	escapeCodes := make([]string, 0, len(escapeTerms)*2)

	//generate escape codes
	for j := range escapeTerms {
		escapeCodes = append(escapeCodes, escapeTerms[j],
			leader+NumLexStr(j, NumOfDigits(len(escapeTerms))))
	}

	return &StringEscaper{
		escaper:   strings.NewReplacer(escapeCodes...),              // NewReplacer copies escapeCodes internally
		unescaper: strings.NewReplacer(SwapEvenOdd(escapeCodes)...), // so here it's safe to swap in place
	}
}

func (e *StringEscaper) Escape(s string) string {
	return e.escaper.Replace(s)
}

func (e *StringEscaper) Unescape(s string) string {
	return e.unescaper.Replace(s)
}
