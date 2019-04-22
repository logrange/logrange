package gorivets

import (
	"errors"
	"strconv"
)

type SortedSlice struct {
	compF CompareF
	data  []interface{}
}

func NewSortedSliceByComp(compF CompareF, initialCapacity int) (*SortedSlice, error) {
	if initialCapacity <= 0 {
		return nil, errors.New("initialCapacity=" + strconv.Itoa(initialCapacity) + " should not be negative integer.")
	}
	return &SortedSlice{compF: compF, data: make([]interface{}, 0, initialCapacity)}, nil
}

func NewSortedSlice(initialCapacity int) (*SortedSlice, error) {
	return NewSortedSliceByComp(ccf, initialCapacity)
}

func NewSortedSliceByParams(data ...interface{}) (*SortedSlice, error) {
	return NewSortedSliceByCompAndParams(ccf, data...)
}

func NewSortedSliceByCompAndParams(compF CompareF, data ...interface{}) (*SortedSlice, error) {
	if data == nil {
		return nil, errors.New("Cannot create SortedSlice from data=nil")
	}
	ss := &SortedSlice{compF: compF, data: make([]interface{}, 0, len(data))}
	for _, val := range data {
		ss.Add(val)
	}
	return ss, nil
}

func (ss *SortedSlice) Len() int {
	return len(ss.data)
}

func (ss *SortedSlice) Add(val interface{}) (int, error) {
	if val == nil {
		return -1, errors.New("val=nil cannot be added to the collection.")
	}

	idx := ss.GetInsertPos(val)
	if idx >= ss.Len() {
		ss.data = append(ss.data, val)
	} else {
		ss.data = append(ss.data, nil)
		copy(ss.data[idx+1:], ss.data[idx:])
		ss.data[idx] = val
	}
	return idx, nil
}

func (ss *SortedSlice) At(idx int) interface{} {
	return ss.data[idx]
}

func (ss *SortedSlice) Find(val interface{}) (int, bool) {
	idx := ss.binarySearch(val)
	return idx, idx >= 0
}

func (ss *SortedSlice) Delete(val interface{}) bool {
	idx := ss.binarySearch(val)
	if idx < 0 {
		return false
	}
	ss.DeleteAt(idx)
	return true
}

func (ss *SortedSlice) DeleteAt(idx int) interface{} {
	result := ss.data[idx]
	ss.data = append(ss.data[:idx], ss.data[idx+1:]...)
	return result
}

func (ss *SortedSlice) Copy() []interface{} {
	c := make([]interface{}, len(ss.data))
	copy(c, ss.data)
	return c
}

func (ss *SortedSlice) GetInsertPos(val interface{}) int {
	len := len(ss.data)
	if len == 0 {
		return 0
	}

	if ss.compF(val, ss.data[len-1]) >= 0 {
		return len
	}

	idx := ss.binarySearch(val)
	if idx < 0 {
		return -(idx + 1)
	}
	return idx
}

func (ss *SortedSlice) binarySearch(val interface{}) int {
	h := len(ss.data) - 1
	l := 0
	for l <= h {
		m := (l + h) >> 1
		vm := ss.data[m]
		c := ss.compF(val, vm)
		switch {
		case c < 0:
			h = m - 1
		case c > 0:
			l = m + 1
		default:
			return m
		}
	}
	return -(l + 1)
}
