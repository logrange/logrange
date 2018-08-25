// data package contains main types and objects used for different data manipulations
package data

type (

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
		Next()

		// Get returns the current record the iterator points to. If there is
		// no current records (all ones are iterated), or the collection is empty
		// the method will return io.EOF in the error.
		//
		// If error is nil, then the method returns slice of bytes, which is the
		// current record. The slice could be nil as well, which is valid.
		Get() ([]byte, error)
	}
)
