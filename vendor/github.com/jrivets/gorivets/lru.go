package gorivets

import (
	"container/list"
	"strconv"
	"time"
)

type (
	// Least Recently Used container interface. Defines basic methods for
	// different implementations (see sized and time-based below).
	//
	// Method calls should be guarded by synchronization primitives in
	// multi-thread environment, unless it's explicitly said "friendly"
	LRU interface {
		// Add an element to the LRU.
		Add(k, v interface{}, size int64)
		// returns the element and marks it as recently used
		Get(k interface{}) (interface{}, bool)
		// returns the element, but not marking it as used recently
		Peek(k interface{}) (interface{}, bool)

		// Deletes an element and call for callback if it is not nil
		// equivavalent to DeleteWithCallback(k, true)
		Delete(k interface{}) interface{}

		// Deletes an elementa and call callback function if callback==true and
		// the callback function was specified for the container
		DeleteWithCallback(k interface{}, callback bool) interface{}

		// Garbage collection call, can be needed for time restricted LRU
		Sweep()
		Clear()
		// Multithread: friendly
		Len() int
		// Multithread: friendly
		Size() int64
	}

	Lru struct {
		list     *list.List
		elements map[interface{}]*list.Element
		size     int64
		maxSize  int64
		callback LruCallback
	}

	element struct {
		key  interface{}
		val  interface{}
		size int64
	}

	lru_ttl struct {
		list     *list.List
		elements map[interface{}]*list.Element
		size     int64
		maxSize  int64
		duration time.Duration
		callback LruCallback
	}

	element_ttl struct {
		key       interface{}
		val       interface{}
		size      int64
		expiredOn time.Time
	}

	LruCallback func(k, v interface{})
)

func NewLRU(maxSize int64, callback LruCallback) LRU {
	if maxSize < 1 {
		panic("LRU size=" + strconv.FormatInt(maxSize, 10) + " should be positive.")
	}
	l := new(Lru)
	l.list = list.New()
	l.elements = make(map[interface{}]*list.Element)
	l.size = 0
	l.maxSize = maxSize
	l.callback = callback
	return l
}

func NewTtlLRU(maxSize int64, duration time.Duration, callback LruCallback) LRU {
	if maxSize < 1 {
		panic("LRU size=" + strconv.FormatInt(maxSize, 10) + " should be positive.")
	}
	l := new(lru_ttl)
	l.list = list.New()
	l.elements = make(map[interface{}]*list.Element)
	l.size = 0
	l.maxSize = maxSize
	l.callback = callback
	l.duration = duration
	return l
}

// =============================== Lru =======================================
func (lru *Lru) Add(k, v interface{}, size int64) {
	lru.Delete(k)
	e := &element{key: k, val: v, size: size}
	el := lru.list.PushBack(e)
	lru.elements[k] = el
	lru.size += size
	for lru.size > lru.maxSize && lru.deleteLast() {
		// left empty intentionally
	}
}

func (lru *Lru) Get(k interface{}) (interface{}, bool) {
	if e, ok := lru.elements[k]; ok {
		lru.list.MoveToBack(e)
		return e.Value.(*element).val, true
	}
	return nil, false
}

func (lru *Lru) Peek(k interface{}) (interface{}, bool) {
	if e, ok := lru.elements[k]; ok {
		return e.Value.(*element).val, true
	}
	return nil, false
}

func (lru *Lru) Delete(k interface{}) interface{} {
	return lru.DeleteWithCallback(k, true)
}

func (lru *Lru) DeleteWithCallback(k interface{}, callback bool) interface{} {
	el, ok := lru.elements[k]
	if !ok {
		return nil
	}
	delete(lru.elements, k)
	e := lru.list.Remove(el).(*element)
	lru.size -= e.size
	if callback && lru.callback != nil {
		lru.callback(e.key, e.val)
	}
	return e.val
}

func (lru *Lru) Sweep() {
}

// Clear the cache. This method will not invoke callbacks for the deleted
// elements
func (lru *Lru) Clear() {
	lru.list.Init()
	lru.elements = make(map[interface{}]*list.Element)
	lru.size = 0
}

func (lru *Lru) Len() int {
	return len(lru.elements)
}

func (lru *Lru) Size() int64 {
	return lru.size
}

func (lru *Lru) deleteLast() bool {
	el := lru.list.Front()
	if el == nil {
		return false
	}
	e := el.Value.(*element)
	lru.Delete(e.key)
	return true
}

// ============================= lru_ttl =====================================
func (lru *lru_ttl) Add(k, v interface{}, size int64) {
	lru.Delete(k)
	now := time.Now()
	e := &element_ttl{key: k, val: v, size: size, expiredOn: now.Add(lru.duration)}
	el := lru.list.PushBack(e)
	lru.elements[k] = el
	lru.size += size
	for (lru.size > lru.maxSize || lru.lastExpired(now)) && lru.deleteLast() {
		// left empty intentionally
	}
}

func (lru *lru_ttl) Get(k interface{}) (interface{}, bool) {
	if e, ok := lru.elements[k]; ok {
		et := e.Value.(*element_ttl)
		et.expiredOn = time.Now().Add(lru.duration)
		lru.list.MoveToBack(e)
		return et.val, true
	}
	return nil, false
}

func (lru *lru_ttl) Peek(k interface{}) (interface{}, bool) {
	if e, ok := lru.elements[k]; ok {
		et := e.Value.(*element_ttl)
		return et.val, true
	}
	return nil, false
}

func (lru *lru_ttl) Delete(k interface{}) interface{} {
	return lru.deleteWithCallback(k, true)
}

func (lru *lru_ttl) DeleteWithCallback(k interface{}, callback bool) interface{} {
	return lru.deleteWithCallback(k, callback)
}

func (lru *lru_ttl) delete(k interface{}) interface{} {
	return lru.deleteWithCallback(k, true)
}

func (lru *lru_ttl) deleteWithCallback(k interface{}, callback bool) interface{} {
	el, ok := lru.elements[k]
	if !ok {
		return nil
	}
	delete(lru.elements, k)
	e := lru.list.Remove(el).(*element_ttl)
	lru.size -= e.size
	if callback && lru.callback != nil {
		lru.callback(e.key, e.val)
	}
	return e.val
}

func (lru *lru_ttl) Sweep() {
	lru.timeCleanup()
}

// Clear the cache. This method will not invoke callbacks for the deleted
// elements
func (lru *lru_ttl) Clear() {
	lru.list.Init()
	lru.elements = make(map[interface{}]*list.Element)
	lru.size = 0
}

func (lru *lru_ttl) Len() int {
	return len(lru.elements)
}

func (lru *lru_ttl) Size() int64 {
	return lru.size
}

func (lru *lru_ttl) deleteLast() bool {
	el := lru.list.Front()
	if el == nil {
		return false
	}
	e := el.Value.(*element_ttl)
	lru.delete(e.key)
	return true
}

func (lru *lru_ttl) lastExpired(now time.Time) bool {
	el := lru.list.Front()
	if el == nil {
		return false
	}
	e := el.Value.(*element_ttl)
	return now.After(e.expiredOn)
}

func (lru *lru_ttl) timeCleanup() {
	now := time.Now()
	for lru.lastExpired(now) && lru.deleteLast() {
		// left empty intentionally
	}
}
