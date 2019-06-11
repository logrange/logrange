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

package model

import (
	"math"
)

type (
	// TimeRange struct defines a time interval.
	TimeRange struct {
		// MinTs contains minimum time in the interval value
		MinTs int64
		// MaxTs contains maximum allowed time value in the interval
		MaxTs int64
	}
)

const (
	// MinTimestamp contains the minimum nanoseconds value
	MinTimestamp = int64(-6795364578871345152)
	MaxTimestamp = math.MaxInt64
)
