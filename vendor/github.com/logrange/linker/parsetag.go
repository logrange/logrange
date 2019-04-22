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

package linker

import (
	"fmt"
	"strings"
)

type parseRes struct {
	val      string
	optional bool
	defVal   string
}

var errTagNotFound = fmt.Errorf("Tag not found")

// parseTag receives a tag name and the tags strings. The tag in the tags is expected
// to be for example:
//
// 	`inject: "componentName, optional: \"abc\"", anotherTag:...`
//
// the tag name separated by collon with its value. Value must be quoted.
// tag value consists of its fields, which are comma separated. The result contains
// the parseRes or an error if any
func parseTag(name, tags string) (parseRes, error) {
	if len(name) >= len(tags) {
		return parseRes{}, errTagNotFound
	}

	i := strings.Index(tags, name)
	if i < 0 {
		return parseRes{}, errTagNotFound
	}

	tags = tags[i+len(name):]
	tags = strings.TrimLeft(tags, " ")
	if len(tags) == 0 || tags[0] != ':' {
		return parseRes{}, fmt.Errorf("Found the tag name=%s, but could not found semicolon after the tag name", name)
	}
	tags = strings.TrimLeft(tags[1:], " ")

	if len(tags) == 0 || tags[0] != '"' {
		return parseRes{}, fmt.Errorf("tag value expected to be in qoutes")
	}
	tags = tags[1:]

	for i = 0; i < len(tags) && tags[i] != '"'; i++ {
		if tags[i] == '\\' {
			i++
		}
	}
	if i >= len(tags) {
		return parseRes{}, fmt.Errorf("tag value expected to be in qoutes, but closing quote is not found.")
	}

	tags = tags[:i]
	var res parseRes
	err := res.parseParams(strings.Split(tags, ","))
	return res, err
}

// parseParams receives slice of string params and turns them into parseParams fields
// supported params:
// optional - if found, then optional field is set true. It can contain
//  default value after a collon if provided.
func (pr *parseRes) parseParams(params []string) error {
	if len(params) == 0 {
		// empty params, does nothing
		return nil
	}

	// name always comes first
	pr.val = params[0]
	for i := 1; i < len(params); i++ {
		v := strings.Trim(params[i], " ")
		if strings.HasPrefix(v, "optional") {
			pr.optional = true
			v = v[len("optional"):]
			v = strings.TrimLeft(v, " ")
			if len(v) > 0 {
				// seems we have default value here
				if v[0] != ':' {
					return fmt.Errorf("optional value should be set via tag value in optional[:<default value>] form, but it is not: %s", params[i])
				}
				pr.defVal = v[1:]
			}
			continue
		}
		return fmt.Errorf("Unknown parameter in the tag value: %s. \"optional\" supported so far.", params[i])
	}

	return nil
}
