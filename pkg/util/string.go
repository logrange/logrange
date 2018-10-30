package util

import (
	"bytes"
	"crypto/rand"
)

func GetRandomString(size int, abc string) string {
	var buffer bytes.Buffer
	var val [64]byte
	var buf []byte

	if size > len(val) {
		buf = make([]byte, size)
	} else {
		buf = val[:size]
	}

	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}

	for _, v := range buf {
		buffer.WriteString(string(abc[int(v)%len(abc)]))
	}

	return buffer.String()
}

// Bytes2String transforms bitstream to a string. val contains bytes and
// only lower bits from any value is used for the calculation. abet - is an
// alpabet which is used for forming the result string.
func Bytes2String(val []byte, abet string, bits int) string {
	kap := len(val) * 8 / bits
	abl := len(abet)
	res := make([]byte, 0, kap)
	mask := (1 << uint(bits)) - 1
	i := 0
	shft := 0
	for i < len(val) {
		b := int(val[i]) >> uint(shft)
		bSize := 8 - shft
		if bSize <= bits {
			i++
			if i < (len(val)) {
				shft = bits - bSize
				b |= int(val[i]) << uint(bSize)
			}
		} else {
			shft += bits
		}
		res = append(res, abet[(b&mask)%abl])
	}
	return string(res)
}

// RemoveDups returns a slice where every element from ss meets only once
func RemoveDups(ss []string) []string {
	j := 0
	found := map[string]bool{}
	for i, s := range ss {
		if !found[s] {
			found[s] = true
			ss[j] = ss[i]
			j++
		}
	}
	return ss[:j]
}
