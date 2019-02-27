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

package container

type (
	// RingBuffer - the ring buffer with fixed capacity. The container has
	// head and tail. It provides operations that allows to manipulate the
	// data stored in the buffer.
	//
	// Handle with caution! The buffer doesn't free elements, and doesn't nilling
	// them intentionally. It was made with an intention to minimize memory allocations
	// for the stored elements. It is the buffer's user responsibility to free
	// and nillify stored values.
	RingBuffer struct {
		v []interface{}
		h int
		n int
	}
)

// NewRingBuffer - returns new ring buffer with size elements reserved
func NewRingBuffer(size int) *RingBuffer {
	if size < 1 {
		panic("size must be positive")
	}
	rb := new(RingBuffer)
	rb.v = make([]interface{}, size, size)
	return rb
}

// Head - returns head's element. Will panic if size of the RingBuffer is 0
func (rb *RingBuffer) Head() interface{} {
	if rb.n == 0 {
		panic("Buffer is empty")
	}
	return rb.v[rb.h]
}

// Tail - returns tail's element. Will panic if size of the RingBuffer is 0
func (rb *RingBuffer) Tail() interface{} {
	if rb.n == 0 {
		panic("Buffer is empty")
	}
	return rb.v[rb.getIdx(rb.h+rb.n-1)]
}

// At - returns element at the index i, countin from the head. Will panic if
// the index is out of bounds
func (rb *RingBuffer) At(i int) interface{} {
	rb.checkIdx(i)
	return rb.v[rb.getIdx(rb.h+i)]
}

// Set - assign value v for the element i, counting from the head.
func (rb *RingBuffer) Set(i int, v interface{}) {
	rb.checkIdx(i)
	rb.v[rb.getIdx(rb.h+i)] = v
}

// Len - returns current buffer size
func (rb *RingBuffer) Len() int {
	return rb.n
}

// Capacity - returns the buffer capacity
func (rb *RingBuffer) Capacity() int {
	return len(rb.v)
}

// AdvanceTail - moves the tail and increases the current buffer size by 1.
// if the buffer size reaches the maximum capacity, it will return head element,
// moving the head and tail both to 1 position.
func (rb *RingBuffer) AdvanceTail() interface{} {
	if rb.n == len(rb.v) {
		rb.h = rb.getIdx(rb.h + 1)
	} else {
		rb.n++
	}
	return rb.v[rb.getIdx(rb.h+rb.n-1)]
}

// AdvanceHead - advances head and reduce the buffer size to 1 (head) element.
// It returns the element, which was at head, before the operation
func (rb *RingBuffer) AdvanceHead() interface{} {
	if rb.n < 1 {
		panic("The buffer is empty")
	}
	rb.n--
	v := rb.v[rb.h]
	rb.h = rb.getIdx(rb.h + 1)
	return v
}

// Push - places value v at the tail. It will increase the buffer size, or pops
// head element if the buffer's capacity is reached. The previos element stored
// at the new tail position is returned. After the operation tail points to the
// new value v.
func (rb *RingBuffer) Push(v interface{}) interface{} {
	r := rb.AdvanceTail()
	rb.v[rb.getIdx(rb.h+rb.n-1)] = v
	return r
}

// IsFull - returns true if the buffer is full Len() == Capacity()
func (rb *RingBuffer) IsFull() bool {
	return rb.n == len(rb.v)
}

// Clear - drops the buffer size to 0
func (rb *RingBuffer) Clear() {
	rb.h = 0
	rb.n = 0
}

func (rb *RingBuffer) getIdx(i int) int {
	if i >= len(rb.v) {
		return i - len(rb.v)
	}
	if i < 0 {
		return len(rb.v) + i
	}
	return i
}

func (rb *RingBuffer) checkIdx(i int) {
	if i < 0 || i >= rb.n {
		panic("Index out of bounds")
	}
}
