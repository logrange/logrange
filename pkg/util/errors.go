package util

import (
	"fmt"
)

var (
	ErrWrongState     = fmt.Errorf("Wrong state, probably already closed.")
	ErrMaxSizeReached = fmt.Errorf("Could not perform the write operation. The maximum size of the storage is reached.")
	ErrNotFound       = fmt.Errorf("The requested object was not found")
)
