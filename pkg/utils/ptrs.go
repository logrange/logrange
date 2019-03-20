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
package utils

// GetInt64Val receives a pointer to int64 value and returns its value or defVal, if the pointer is nil
func GetInt64Val(ptr *int64, defVal int64) int64 {
	if ptr != nil {
		return *ptr
	}
	return defVal
}

func GetStringVal(ptr *string, defVal string) string {
	if ptr != nil {
		return *ptr
	}
	return defVal
}

// returns (ptr value, ok), ok == false if ptr is nil
func PtrBool(b *bool) (bool, bool) {
	if b == nil {
		return false, false
	}
	return *b, true
}

func BoolPtr(b bool) *bool {
	return &b
}

// returns (ptr value, ok), ok == false if ptr is nil
func PtrInt(i *int) (int, bool) {
	if i == nil {
		return 0, false
	}
	return *i, true
}

func IntPtr(i int) *int {
	return &i
}
