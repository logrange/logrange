package util

import (
	"bytes"
	"encoding/json"
	"strings"
)

// ToJsonStr encodes v to a json string. Don't use it in data streaming due to
// bad performance and memory allocations
func ToJsonStr(v interface{}) string {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(v)
	if err != nil {
		return ""
	}
	s := string(buffer.Bytes())
	return strings.TrimRight(s, "\n")
}
