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

package bytes

// Pool manages a pool of slices of bytes. It is supposed to use the pool in
// highly loaded apps where slice of bytes are needed often. An example could be
// handling a network packages.
type Pool struct {
}

func (p *Pool) Arrange(size int) []byte {
	b := make([]byte, size)
	return b
}

func (p *Pool) Release(buf []byte) {

}
