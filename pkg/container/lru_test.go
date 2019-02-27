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

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkLruLocal(b *testing.B) {
	l := NewLru(1000, time.Second, nil)
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Put(rand.Intn(1000), rand.Intn(150), 1)
		l.Get(rand.Intn(1000))
	}
}

func TestLruSimple(t *testing.T) {
	l := NewLru(1000, time.Hour, nil)
	l.Put("a", 23, 100)
	l.Put("b", 23, 100)
	if l.Len() != 2 {
		t.Fatal("expecting lru len == 2, but len=", l.Len())
	}
	if l.Size() != 200 {
		t.Fatal("expecting lru size == 200, but it is ", l.Size())
	}

	l.Put("a", 23, 50)
	if l.Len() != 2 {
		t.Fatal("expecting lru len == 2, but len=", l.Len())
	}
	if l.Size() != 150 {
		t.Fatal("expecting lru size == 150, but it is ", l.Size())
	}
}

func TestLruSize(t *testing.T) {
	i := 0
	arr := []string{"b", "bb", "a", "c"}
	l := NewLru(1000, time.Hour, func(k, v interface{}) {
		ks := k.(string)
		if ks != arr[0] {
			t.Fatal("expecting key=", arr[0], ", k=", ks)
		}
		arr = arr[1:]
		i++
	})
	l.Put("a", 23, 500)
	l.Put("b", 23, 250)
	l.Put("bb", 23, 250)
	l.Get("bb")
	l.Get("a")
	if l.Len() != 3 {
		t.Fatal("expecting lru len == 2, but len=", l.Len())
	}
	if l.Size() != 1000 {
		t.Fatal("expecting lru size == 1000, but it is ", l.Size())
	}
	if i != 0 {
		t.Fatal("expecting i=0, but i=", i)
	}

	l.Put("c", 54, 500)
	if l.Len() != 2 {
		t.Fatal("expecting lru len == 2, but len=", l.Len())
	}
	if i != 2 {
		t.Fatal("expecting i=2, but i=", i)
	}

	l.Put("d", 54, 1000)
	if l.Len() != 1 {
		t.Fatal("expecting lru len == 1, but len=", l.Len())
	}
	if i != 4 {
		t.Fatal("expecting i=4, but i=", i)
	}
}

func TestLruDelete(t *testing.T) {
	arr := []string{"bb", "aa", "a", "b", "c", "d", "bbb"}
	l := NewLru(1000, time.Hour, func(k, v interface{}) {
		ks := k.(string)
		if ks != arr[0] {
			t.Fatal("expecting key=", arr[0], ", k=", ks)
		}
		arr = arr[1:]
	})
	l.Put("a", 23, 250)
	l.Put("aa", 23, 250)
	l.Put("b", 23, 250)
	l.Put("bb", 23, 250)
	l.Delete("bb")
	l.Delete("aa")
	l.Put("c", 23, 250)
	l.Put("d", 23, 250)
	l.Put("bbb", 23, 10300)
}

func TestLruTimeout(t *testing.T) {
	l := NewLru(1000, time.Millisecond*10, nil)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	if l.Len() != 2 {
		t.Fatal("Must have 2 elements")
	}

	time.Sleep(10 * time.Millisecond)
	l.SweepByTime()
	if l.Len() != 0 || l.Get(1) != nil || l.Get(2) != nil {
		t.Fatal("Must have 0 elements")
	}
}

func TestLruNilTimeout(t *testing.T) {
	l := NewLru(1000, 0, nil)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	if l.Len() != 2 {
		t.Fatal("Must have 2 elements")
	}

	time.Sleep(10 * time.Millisecond)
	l.SweepByTime()
	if l.Len() != 2 || l.Get(1) == nil || l.Get(2) == nil {
		t.Fatal("It must still have 2 elements")
	}
}

func TestLruDeleteOrder(t *testing.T) {
	l := NewLru(3, time.Hour, nil)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	l.Put(3, 3, 2)
	if l.Len() != 2 || l.Get(1) != nil || l.Get(2) == nil {
		t.Fatal("Must have 2 elements")
	}

	l.Put(1, 1, 1)
	if l.Len() != 2 || l.Get(3) != nil || l.Get(1) == nil || l.Get(2) == nil {
		t.Fatal("Must have 2 elements")
	}

}

func TestLruDeleteOrder2(t *testing.T) {
	l := NewLru(3, time.Hour, nil)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	l.Put(3, 3, 2)
	if l.Len() != 2 || l.Peek(1) != nil || l.Peek(2) == nil {
		t.Fatal("Must have 2 elements")
	}

	l.Put(1, 1, 1)
	if l.Len() != 2 || l.Peek(3) == nil || l.Peek(1) == nil || l.Get(2) != nil {
		t.Fatal("Must have 2 elements")
	}
}

func TestLruAddToList(t *testing.T) {
	h := addToHead(nil, nil)
	if h != nil {
		t.Fatal("Wrong nil, nil adding result")
	}

	e := new(lruElement)
	h = addToHead(nil, e)
	if h != e || h.next != e || h.prev != e {
		t.Fatal("Incorrect list")
	}

	h = addToHead(h, nil)
	if h != e || h.next != e || h.prev != e {
		t.Fatal("Incorrect list (2)")
	}

	e1 := new(lruElement)
	h = addToHead(h, e1)
	if h != e1 || h.next != e || h.prev != e || e.prev != h || e.next != h {
		t.Fatal("Incorrect list (3)")
	}
}

func TestLruRemoveFromList(t *testing.T) {
	e3 := new(lruElement)
	e2 := new(lruElement)
	e1 := new(lruElement)
	h := addToHead(nil, e3)
	h = addToHead(h, e2)
	h = addToHead(h, e1)

	h = removeFromList(h, e2)
	if h != e1 || h.next != e3 || h.prev != e3 || e3.prev != h || e3.next != h {
		t.Fatal("Incorrect list ")
	}

	h = removeFromList(h, e1)
	if h != e3 || h.next != e3 || h.prev != e3 {
		t.Fatal("Incorrect list (2)")
	}
	h = removeFromList(h, e3)
	if h != nil {
		t.Fatal("Incorrect list (3)")
	}
}

func TestLruIterate(t *testing.T) {
	l := NewLru(3, time.Hour, nil)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	l.Put(3, 3, 1)

	i := 3
	l.Iterate(func(k, v interface{}) bool {
		if k.(int) != i {
			t.Fatal("Expecting ", i, ", but got ", k)
		}
		i--
		return true
	})
	l.Put(4, 4, 1)
	l.Get(3)
	l.Get(2)
	l.Get(1) // should be pulled out
	i = 2
	l.Iterate(func(k, v interface{}) bool {
		if k.(int) != i {
			t.Fatal("Expecting ", i, ", but got ", k)
		}
		i++
		return true
	})

	// now visit only first one
	i = 2
	l.Iterate(func(k, v interface{}) bool {
		if k.(int) != i {
			t.Fatal("Expecting ", i, ", but got ", k)
		}
		return false
	})
}

func TestLruGetPeek(t *testing.T) {
	l := NewLru(3, time.Hour, nil)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	l.Put(3, 3, 1)

	if l.Get(4) != nil {
		t.Fatal("Should not be 4 in the container")
	}

	v := l.Get(3)
	ts := v.TouchedAt()
	time.Sleep(10 * time.Microsecond)
	if l.Peek(3).TouchedAt() != ts {
		t.Fatal("Peek should not affect ts")
	}
	if l.Get(3).TouchedAt() == ts {
		t.Fatal("Get should affect ts")
	}
}

func TestLruClear(t *testing.T) {
	i := 0
	f := func(k, v interface{}) {
		i++
	}

	l := NewLru(30, time.Hour, f)
	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	l.Put(3, 3, 2)
	if l.Len() != 3 {
		t.Fatal("Must have 3 elements")
	}

	l.Clear(false)
	if i != 0 || l.Size() != 0 || l.Len() != 0 {
		t.Fatal("clear doesn't work properly i=", i)
	}

	l.Put(1, 1, 1)
	l.Put(2, 2, 1)
	l.Put(3, 3, 2)
	l.Clear(true)
	if i != 3 || l.Size() != 0 || l.Len() != 0 {
		t.Fatal("clear doesn't work properly i=", i)
	}

}
