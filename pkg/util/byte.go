package util

// BytesCopy makes a copy of src slice of bytes
func BytesCopy(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	b := make([]byte, len(src))
	copy(b, src)
	return b
}
