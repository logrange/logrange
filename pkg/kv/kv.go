// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
kv package contains interfaces and structures for working with a key-value storage.
The kv.Storage can be implemented as a distributed consistent storage like etcd,
consul etc. For stand-alone or test environments some light-weight implementations
like local file-storage or in memory implementations could be used.
*/

package kv

import (
	"context"
	"fmt"
	"time"
)

type (

	// LeaseId is a record lease identifier. Every record in the
	// storage can have a lease id or has NoLeaseId. Records that have a lease
	// will be automatically deleted if the lease is expired.
	LeaseId int64

	// Lessor is an interface which allows to manage leases in conjuction
	// with the Storage
	Lessor interface {

		// NewLease creates a new lease with ttl provided. The keepAlive flag
		// indicates that the the lease must be kept alive until the current process
		// is done.
		NewLease(ctx context.Context, ttl time.Duration, keepAlive bool) (Lease, error)

		// GetLease returns a Lease object by it's Id
		GetLease(lid LeaseId) (Lease, error)
	}

	// Lease allows to control a lease. Lease is an abstraction which identifies
	// a time interval where the lease is valid - ttl. Storage records could be
	// associated with the lease by its Id.
	Lease interface {

		// Id returns the lease Id
		Id() LeaseId

		// TTL is time to the lease expiration
		TTL() time.Duration

		// Release terminates (releases) the lease immediately
		Release() error
	}

	// A record version identifier
	Version int64

	// Key is the type for storing keys
	Key string

	// Value type is for storing Values
	Value []byte

	// A record that can be stored in a KV
	Record struct {
		// Key is a key for the record
		Key Key
		// Value is a value for the record
		Value Value

		// A version that identifies the record. It is managed by the Storage and
		// it is ignored in Create and update operations
		Version Version

		// Lease contains the record leaseId. If the lease is not empty
		// (Lease != NoLeaseId), the record availability is defined by the
		// lease TTL. As soon as the lease becomes invalid, the record is
		// Removed from the storage permanently
		//
		// Records with NoLeaseId should be deleted explicitly. The value
		// can be set only when the record is created and cannot be changed
		// (it will be ignored) during updates.
		Lease LeaseId
	}

	// Storage interface defines some operations over the record storage.
	// The record storage allows to keep key-value pairs, and supports a set
	// of operations that allow to implement some distributed (if supported) primitives
	Storage interface {
		// Lessor returns Lessor implementation for the storage
		Lessor() Lessor

		// Create adds a new record into the storage. It returns existing record with
		// ErrAlreadyExists error if it already exists in the storage.
		// Create returns version of the new record with error=nil
		Create(ctx context.Context, record Record) (Version, error)

		// Get retrieves the record by its key. It will return nil and an error,
		// which will indicate the reason why the operation was not succesful.
		// ErrNotFound is returned if the key is not found in the storage
		Get(ctx context.Context, key Key) (Record, error)

		// GetRange returns the list of records for the range of keys (inclusively)
		GetRange(ctx context.Context, startKey, endKey Key) (Records, error)

		// CasByVersion compares-and-sets the record Value if the record stored
		// version is same as in the provided record. The record version will be updated
		// too and returned in the result.
		//
		// the record lease WILL NOT BE CHANGED, so the record will be associated
		// with the lease, that it has been done before.
		//
		// an error will contain the reason if the operation was not successful, or
		// the new version will be returned otherwise
		CasByVersion(ctx context.Context, record Record) (Version, error)

		// Delete removes the record from the storage by its key. It returns
		// an error if the operation was not successful.
		Delete(ctx context.Context, key Key) error

		// WaitForVersionChange will wait for the record version change. The
		// version param contans an expected record version. The call returns
		// immediately if the record is not found together with ErrNotFound, or
		// if the record version is different than expected (no error this case
		// is returned).
		//
		// the call will block the current go-routine until one of the following
		// things happens:
		// - context is closed
		// - the record version is changed
		// - the record is deleted
		//
		// If the record is deleted during the call (nil, nil) is returned
		WaitForVersionChange(ctx context.Context, key Key, version Version) (Record, error)
	}

	Records []Record
)

const NoLeaseId = 0

var (
	ErrAlreadyExists = fmt.Errorf("The key already exists")
	ErrNotFound      = fmt.Errorf("The value is not found")
	ErrWrongVersion  = fmt.Errorf("Wrong version is provided")
	ErrWrongLeaseId  = fmt.Errorf("Wrong leaseId")
)

// Copy returns copy of the record r
func (r Record) Copy() Record {
	var res Record
	res.Key = r.Key

	if r.Value != nil {
		res.Value = make([]byte, len(r.Value))
		copy(res.Value, r.Value)
	}
	res.Lease = r.Lease
	res.Version = r.Version
	return res
}

// Len is part of sort.Interface.
func (rcs Records) Len() int {
	return len(rcs)
}

// Swap is part of sort.Interface.
func (rcs Records) Swap(i, j int) {
	rcs[i], rcs[j] = rcs[j], rcs[i]
}

// Less is part of sort.Interface.
func (rcs Records) Less(i, j int) bool {
	return rcs[i].Key < rcs[j].Key
}
