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
package errors

import (
	"fmt"
)

var (
	WrongState     = fmt.Errorf("Wrong state, expected another one")
	MaxSizeReached = fmt.Errorf("Could not perform the write operation. The maximum size of the storage is reached.")
	NotFound       = fmt.Errorf("The requested object is not found")
	ClosedState    = fmt.Errorf("The component state is closed")
	IsNotEmpty     = fmt.Errorf("Is not empy")
	AlreadyExists  = fmt.Errorf("The object already exists")
)
