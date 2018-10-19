package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBytesCopy(t *testing.T) {
	e := []byte{0, 1, 2, 3}
	a := BytesCopy(e)

	assert.NotNil(t, a)
	assert.False(t, &a == &e)
	assert.Equal(t, cap(a), cap(e))
}
