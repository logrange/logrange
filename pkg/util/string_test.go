package util

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestRemoveDups(t *testing.T) {
	e := []string{"a", "b", "c", "d"}
	a := RemoveDups([]string{"a", "a", "b", "c", "b", "c", "d"})

	assert.NotNil(t, a)
	assert.ElementsMatch(t, a, e)
}
