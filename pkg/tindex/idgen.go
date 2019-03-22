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

package tindex

import (
	"fmt"
	"github.com/logrange/logrange/pkg/utils"
)

func newSrc() string {
	id := utils.NextSimpleId()
	// making 2 last digits changing from one id to another to be sure the journals will go to different sub-folders
	// (see NextSimpleId() implementation for details)
	return fmt.Sprintf("%X%02X", id, (id>>16)&0xFF)
}
