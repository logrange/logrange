// records package contains main types and objects used for different data
// manipulations. Record is a slice of bytes with unknown semantic. It means
// that the package doesn't care about what the records are, but just describes
// the main types the records selecting like records stores and iterators.
package records

import (
	"context"
)

type (
	// Record is a slice of byte, which contains the record information.
	Record []byte

	// Iterator allows to iterate over a collection of records. The interface
	// allows to select records in an order (either forward, backward or any
	// other one) and returns them via Get() function.
	//
	// When the iterator is initialized it points to the record, which will be
	// selected first. Certain implementation defines an order, so the implementation
	// defines which record will be returned first, which one is second etc.
	//
	// If no more records could be returned the Get() function will return io.EOF
	Iterator interface {

		// Next moves the iterator current position to the next record. Implementation
		// must define the order and which record should be the next.
		//
		// Next expects ctx to be used in case of the call is going to be blocked
		// some implementations can ignore this parameter if the the
		// operation can be perform without blocking the context.
		//
		// For some implementations, calling the function makes the result, returned
		// by previous call of Get() not relevant. It means, that NO results,
		// that were previously received by calling Get(), must be used after
		// the Next() call. In case of if the result is needed it must be copied
		// to another record before calling the Next() function.
		Next(ctx context.Context)

		// Get returns the current record the iterator points to. If there is
		// no current records (all ones are iterated), or the collection is empty
		// the method will return io.EOF in the error.
		//
		// Get receives ctx to be used in case of the call is going to be blocked.
		// Some implementations can ignore this parameter if the the
		// operation can be perform without blocking the context.
		//
		// If error is nil, then the method returns slice of bytes, which is the
		// current record. The slice could be nil as well, which is valid.
		Get(ctx context.Context) (Record, error)
	}
)
