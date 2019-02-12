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

package container

// CLElement struct describes a circular list element. It contains references
// to previous and next elements. By default every non-nil element forms a circular
// list which contains only one element, which refers to itself.
type CLElement struct {
	prev, next *CLElement
	Val        interface{}
}

// NewCLElement returns new CLElement which refers to itself
func NewCLElement() *CLElement {
	cle := new(CLElement)
	cle.next = cle
	cle.prev = cle
	return cle
}

// Append adds chain to the list after the cle
func (cle *CLElement) Append(chain *CLElement) *CLElement {
	if chain == nil {
		return cle
	}
	if cle == nil {
		return chain
	}
	n := cle.next
	cle.next = chain
	chp := chain.prev
	n.prev = chp
	chain.prev = cle
	chp.next = n
	return cle
}

// Len returns number of elements in the Circular list. It has O(N) complexity.
func (cle *CLElement) Len() int {
	if cle == nil {
		return 0
	}
	cnt := 1
	c := cle.next
	for c != cle {
		cnt++
		c = c.next
	}
	return cnt
}

// TearOff removes e from the list which probably heads by cle. It returns new head, which is cle.next, if cle is e,
// or the cle itself otherwise
func (cle *CLElement) TearOff(e *CLElement) *CLElement {
	if e == nil {
		return cle
	}
	if e == cle && cle.next == cle {
		return nil
	}
	res := cle
	if res == e {
		res = cle.next
	}
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = e
	e.next = e
	return res
}
