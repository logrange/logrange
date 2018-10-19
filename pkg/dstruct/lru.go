package dstruct

import (
	"time"
)

type (

	// Lru is "Least Recently Used" container, which keeps key-value pairs in it.
	// Lru supports auto-removing discipline which can be triggered by the container
	// size, an element expiration timeout or both. It sorts stored elements
	// by their recently used time, so most recently used are stored at head
	// of the list, but auto-removing procedure works from the bottom, so it removes
	// longest not-used element first.
	//
	// Lru should be properly synchronized in case of it is used from multiple
	// goroutines.
	Lru struct {
		head    *lruElement
		pool    *lruElement
		kvMap   map[interface{}]*lruElement
		size    int64
		maxSize int64
		maxDur  time.Duration
		cback   LruDeleteCallback
	}

	// LruValue represents a value, stored in Lru. The object is returned by
	// Lru.Get() function.
	LruValue struct {
		size int64
		ts   time.Time
		key  interface{}
		val  interface{}
	}

	LruDeleteCallback func(k, v interface{})
	LruCallback       func(k, v interface{}) bool

	lruElement struct {
		prev *lruElement
		next *lruElement
		v    LruValue
	}
)

var nilTime = time.Time{}

// NewLru creates new Lru container with maximum size maxSize, and maximum
// time 'to' an element can stay in the cache. cback is a function which is
// invoked when an element is pulled out of the cache. It can be nil
//
// Timeout 'to' could be 0, what means don't use it at all
func NewLru(maxSize int64, to time.Duration, cback LruDeleteCallback) *Lru {
	l := new(Lru)
	l.kvMap = make(map[interface{}]*lruElement)
	l.maxSize = maxSize
	l.maxDur = to
	l.cback = cback
	return l
}

// Put places new value v with the key k, considering the size of the element
// as 'size'. Some elements can be pull out from the Lru due to their timeouts
// or if the collection size will exceed the max value.
func (l *Lru) Put(k, v interface{}, size int64) {
	e, ok := l.kvMap[k]
	if ok {
		// we had another element e, found by k, deleting it first
		l.delete(e, true)
	}

	tm := l.SweepByTime()
	l.sweepBySize(size)

	// avoid allocation for the element. We could have the object cached, so will
	// use it, if we have one.
	if l.pool != nil {
		e = l.pool
		l.pool = nil
	} else {
		e = new(lruElement)
	}

	e.v.key = k
	e.v.val = v
	e.v.ts = tm
	e.v.size = size
	l.head = addToHead(l.head, e)
	l.kvMap[k] = e
	l.size += size
}

// Get returns the *LruValue by its key, and removes it from the collection.
// The resulted value could be valid
// till ANY other call to the Lru, and its value is undefined after that.
// Please use the value as soon as you get it and never cache or pass it through
// to other functions.
func (l *Lru) Get(k interface{}) *LruValue {
	ts := l.SweepByTime()
	e, ok := l.kvMap[k]
	if ok {
		l.head = removeFromList(l.head, e)
		l.head = addToHead(l.head, e)
		e.v.ts = ts
		return &e.v
	}
	return nil
}

// Peek works the same way like Get() does, but it doesn't remove the element
// from the Lru
func (l *Lru) Peek(k interface{}) *LruValue {
	l.SweepByTime()
	e, ok := l.kvMap[k]
	if ok {
		return &e.v
	}
	return nil
}

// Delete removes the value by its key 'k' from the Lru. If the container has
// callbacks, it will notify listeners about the operation
func (l *Lru) Delete(k interface{}) {
	l.SweepByTime()
	e, ok := l.kvMap[k]
	if ok {
		l.delete(e, true)
	}
}

// DeleteNoCallback removes the value by its key 'k' from the Lru. No notifications
// will be done.
func (l *Lru) DeleteNoCallback(k interface{}) {
	l.SweepByTime()
	e, ok := l.kvMap[k]
	if ok {
		l.delete(e, false)
	}
}

// Clear removes all elements from the container. It will notify listeners about
// any element which is being deleted if cb == true.
func (l *Lru) Clear(cb bool) {
	for l.head != nil {
		l.delete(l.head, cb)
	}
}

// Iterate is the container visitor which walks over the elements in LRU order.
// It calls f() for every key-value pair and continues until the f() returns false,
// or all elements are visited.
//
// Note: the modifications of the container must not allowed in the f. It means
// f MUST not use the Lru functions.
func (l *Lru) Iterate(f LruCallback) {
	h := l.head
	for h != nil {
		if !f(h.v.key, h.v.val) {
			break
		}
		h = h.next
		if h == l.head {
			break
		}
	}
}

// Size returns current size
func (l *Lru) Size() int64 {
	return l.size
}

// Len returns number or elements that are in the container
func (l *Lru) Len() int {
	return len(l.kvMap)
}

// SweepByTime walks trhough the container values and removes that are expired
func (l *Lru) SweepByTime() time.Time {
	if l.maxDur == 0 {
		return nilTime
	}
	tm := time.Now()
	for l.head != nil && tm.Sub(l.head.prev.v.ts) > l.maxDur {
		last := l.head.prev
		l.delete(last, true)
	}
	return tm
}

// GetData returns underlying container data for the LRU. The returned value
// is a key-value map
func (l *Lru) GetData() map[interface{}]interface{} {
	res := make(map[interface{}]interface{}, len(l.kvMap))
	for k, v := range l.kvMap {
		res[k] = v.v.val
	}
	return res
}

func (l *Lru) sweepBySize(addSize int64) {
	for l.head != nil && l.size+addSize > l.maxSize {
		last := l.head.prev
		l.delete(last, true)
	}
}

func (l *Lru) delete(e *lruElement, cb bool) {
	l.head = removeFromList(l.head, e)
	l.size -= e.v.size
	l.pool = e
	delete(l.kvMap, e.v.key)
	if cb && l.cback != nil {
		l.cback(e.v.key, e.v.val)
	}
	e.v.key = nil
	e.v.val = nil
}

func removeFromList(head, e *lruElement) *lruElement {
	if e == head && head.next == head {
		head = nil
	}
	e.prev.next = e.next
	e.next.prev = e.prev
	if e == head {
		head = e.next
	}
	return head
}

// add n to list with head and returns new head
func addToHead(head *lruElement, n *lruElement) *lruElement {
	if n == nil {
		return head
	}
	if head == nil {
		n.prev = n
		n.next = n
		return n
	}
	n.next = head
	n.prev = head.prev
	head.prev = n
	n.prev.next = n
	return n
}

func (v *LruValue) Key() interface{} {
	return v.key
}

func (v *LruValue) Val() interface{} {
	return v.val
}

func (v *LruValue) Size() int64 {
	return v.size
}

func (v *LruValue) TouchedAt() time.Time {
	return v.ts
}
