package utils

func PtrBool(b *bool) (bool, bool) {
	if b == nil {
		return false, false
	}
	return *b, true
}

func BoolPtr(b bool) *bool {
	return &b
}

func PtrInt(i *int) (int, bool) {
	if i == nil {
		return 0, false
	}
	return *i, true
}

func IntPtr(i int) *int {
	return &i
}
