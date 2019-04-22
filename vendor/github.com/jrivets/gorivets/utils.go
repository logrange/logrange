package gorivets

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

// CompareF is used in collections to compare elements if some ordering is needed
// please see `SortedSlice` as an example. It compares 2 objects a and b and returns a value(val): val < 0 if a < b, val == 0 if a == b, and val > 0 if a > b
type CompareF func(a, b interface{}) int
type Int64 int64

type Comparable interface {
	Compare(other Comparable) int
}

func (i Int64) Compare(other Comparable) int {
	i2 := other.(Int64)
	switch {
	case i < i2:
		return -1
	case i > i2:
		return 1
	default:
		return 0
	}
}

var ccf = func(a, b interface{}) int {
	return a.(Comparable).Compare(b.(Comparable))
}

// Returns minimal value of two integers provided
func Min(a, b int) int {
	if a < b {
		return a
	} else if b < a {
		return b
	}
	return a
}

// Returns a maximal value of two integers provided
func Max(a, b int) int {
	if a > b {
		return a
	} else if b > a {
		return b
	}
	return a
}

func AbsInt64(val int64) int64 {
	if val >= 0 {
		return val
	}
	return -val
}

func CompareInt(a int, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Calls recover() to consume panic if it happened, it is recomended to be used with defer:
//
// 	func TestNoPanic() {
//		defer EndQuietly()
// 		panic()
//	}
func EndQuietly() {
	recover()
}

// Calls panic with the error description if the err is not nil
func AssertNoError(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func AssertNotNil(inf interface{}) {
	AssertNotNilMsg(inf, "nil")
}

func AssertNotNilMsg(inf interface{}, msg string) {
	if IsNil(inf) {
		panic(msg)
	}
}

func IsNil(inf interface{}) bool {
	if inf == nil {
		return true
	}
	if !reflect.ValueOf(inf).Elem().IsValid() {
		return true
	}
	return false
}

// Invokes the function and returns whether it panics or not
func CheckPanic(f func()) (result interface{}) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		result = r
	}()
	f()
	return
}

// Parses boolean value removing leading or trailing spaces if present
func ParseBool(value string, defaultValue bool) (bool, error) {
	value = strings.ToLower(strings.Trim(value, " "))
	if value == "" {
		return defaultValue, nil
	}

	return strconv.ParseBool(value)
}

// Parses string to int value, see ParseInt64
func ParseInt(value string, min, max, defaultValue int) (int, error) {
	res, err := ParseInt64(value, int64(min), int64(max), int64(defaultValue))
	return int(res), err
}

var (
	cKbScale  = []string{"kb", "mb", "gb", "tb", "pb"}
	cKScale   = []string{"k", "m", "g", "t", "p"}
	cKibScale = []string{"kib", "mib", "gib", "tib", "pib"}
)

// Formats val as 1000 power scale ("kb", "mb", "gb", "tb", "pb"), or 1024 power
// ("kib", "mib", "gib", "tib", "pib"), or just removes value as is
func FormatInt64(val int64, scale int) string {
	scl := int64(scale)
	var sfx []string
	if scale == 1000 {
		sfx = cKbScale
	} else if scale == 1024 {
		sfx = cKibScale
	} else {
		return strconv.FormatInt(val, 10)
	}
	idx := -1
	frac := scl / 2
	if val < 0 {
		frac = -frac
	}
	for idx < len(sfx)-1 && AbsInt64(val) > scl {
		val = (val + frac) / scl
		idx++
	}

	if idx >= 0 {
		return strconv.FormatInt(val, 10) + sfx[idx]
	}
	return strconv.FormatInt(val, 10)
}

// ParseInt64 tries to convert value to int64, or returns default if the value is empty string.
// The value can look like "12Kb" which means 12*1000, or "12Kib" which means 12*1024. The following
// suffixes are supported for scaling by 1000 each: "kb", "mb", "gb", "tb", "pb", or "k", "m", "g", "t", "p".
// For 1024 scale the following suffixes are supported: "kib", "mib", "gib", "tib", "pib"
func ParseInt64(value string, min, max, defaultValue int64) (int64, error) {
	if defaultValue < min || defaultValue > max || max < min {
		return 0, errors.New("Inconsistent arguments provided min=" + strconv.FormatInt(min, 10) +
			", max=" + strconv.FormatInt(max, 10) + ", defaultVelue=" + strconv.FormatInt(defaultValue, 10))
	}
	value = strings.ToLower(strings.Trim(value, " "))
	if value == "" {
		return defaultValue, nil
	}

	value, scale := parseSuffixVsScale(value, cKbScale, 1000)
	if scale == 1 {
		value, scale = parseSuffixVsScale(value, cKScale, 1000)
		if scale == 1 {
			value, scale = parseSuffixVsScale(value, cKibScale, 1024)
		}
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	val := int64(intValue) * scale

	if min > val || max < val {
		return 0, errors.New("Value should be in the range [" + strconv.FormatInt(min, 10) + ".." + strconv.FormatInt(max, 10) + "]")
	}

	return val, nil
}

func parseSuffixVsScale(value string, suffixes []string, scale int64) (string, int64) {
	idx, str := getSuffix(value, suffixes)
	if idx < 0 {
		return value, 1
	}
	val := scale
	for ; idx > 0; idx-- {
		val *= scale
	}
	return value[:len(value)-len(str)], val
}

func getSuffix(value string, suffixes []string) (int, string) {
	for idx, sfx := range suffixes {
		if strings.HasSuffix(value, sfx) {
			return idx, sfx
		}
	}
	return -1, ""
}
