package dstruct

import (
	"sync"
	"testing"

	"math/rand"
	"time"
)

func TestGeneral(t *testing.T) {
	r := NewRingBuffer(3)
	if r.Len() != 0 || r.Capacity() != 3 {
		t.Fatal("wrong constrains")
	}
	r.Push(int(1))
	r.Push(int(2))
	r.Push(int(3))
	if r.Len() != 3 {
		t.Fatal("wrong size, must be 3, but ", r.Len())
	}

	if r.AdvanceHead().(int) != 1 {
		t.Fatal("Expecting 1")
	}
	r.Push(int(4))

	if r.AdvanceTail().(int) != 2 {
		t.Fatal("Expecting 2")
	}

	if r.At(0).(int) != 3 || r.At(1).(int) != 4 || r.At(2).(int) != 2 {
		t.Fatal("Wrong values ", r.v)
	}

	r.Set(2, 5)
	if r.Tail().(int) != 5 || r.At(2) != 5 {
		t.Fatal("Wrong values ", r.v)
	}
}

func TestPanicing(t *testing.T) {
	if !catch(func() { NewRingBuffer(0) }) {
		t.Fatal("Expecting panic - wrong size")
	}

	r := NewRingBuffer(5)
	if !catch(func() { r.Head() }) {
		t.Fatal("Expecting panic - head on 0 sized buf")
	}

	if !catch(func() { r.Tail() }) {
		t.Fatal("Expecting panic - tail on 0 sized buf")
	}

	if !catch(func() { r.AdvanceHead() }) {
		t.Fatal("Expecting panic - advance head on 0 sized buf")
	}

	if !catch(func() { r.At(0) }) {
		t.Fatal("Expecting panic - at on 0 sized buf")
	}

	r.Push(1)
	if catch(func() { r.At(0) }) || !catch(func() { r.At(1) }) {
		t.Fatal("Expecting panic - index out of boundx")
	}
}

func BenchmarkPush(b *testing.B) {
	var m sync.Mutex
	rand.Seed(time.Now().UnixNano())
	rb := NewRingBuffer(12000)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r := rand.Intn(100) + 1
		for i := 0; i < r; i++ {
			m.Lock()
			rb.Push(i + r)
			m.Unlock()
		}
	}
}

func BenchmarkPushNS(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	rb := NewRingBuffer(12000)
	rb2 := NewRingBuffer(12000)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r := rand.Intn(100) + 1
		for i := 0; i < r; i++ {
			rb.Push(i + r)
		}
		for i := 0; i < r; i++ {
			rb2.Push(i + r)
		}
	}
}

func catch(f func()) (v bool) {
	defer func() {
		v = recover() != nil
	}()
	f()
	return v
}
