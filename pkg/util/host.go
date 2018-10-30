package util

import (
	"fmt"
)

// HostId must be unique identifier in multi-host environment. The id could be
// used for generating some cross-cluster unique identifiers like chunk id etc.
var HostId16 uint16

var (
	ErrWrongState     = fmt.Errorf("Wrong state, probably already closed.")
	ErrMaxSizeReached = fmt.Errorf("Could not perform the write operation. The maximum size of the storage is reached.")
)
