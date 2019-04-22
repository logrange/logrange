// Copyright 2018-2019 The logrange Authors
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
Package inmem contains in-memory implementation of kv.Storage and other objects to
work with key-value storage
*/
package inmem

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/logrange/range/pkg/kv"
)

type (
	mStorage struct {
		lsId    kv.LeaseId
		lock    sync.Mutex
		data    map[kv.Key]kv.Record
		leases  map[kv.LeaseId]*lease
		waiters map[kv.Key][]chan struct{}
	}

	lease struct {
		ms      *mStorage
		id      kv.LeaseId
		ttl     time.Duration
		endTime time.Time
		cch     chan struct{}
	}
)

func New() *mStorage {
	ms := new(mStorage)
	ms.data = make(map[kv.Key]kv.Record)
	ms.leases = make(map[kv.LeaseId]*lease)
	ms.waiters = make(map[kv.Key][]chan struct{})
	return ms
}

// ------------------------------ Storage ------------------------------------
func (ms *mStorage) Lessor() kv.Lessor {
	return ms
}

// Create adds a new record into the storage. It returns existing record with
// ErrAlreadyExists error if it already exists in the storage.
// Create returns version of the new record with error=nil
func (ms *mStorage) Create(ctx context.Context, record kv.Record) (kv.Version, error) {
	if len(record.Key) == 0 {
		return 0, fmt.Errorf("Could not add a record with empty key")
	}

	ms.lock.Lock()
	if _, ok := ms.data[record.Key]; ok {
		ms.lock.Unlock()
		return 0, kv.ErrAlreadyExists
	}

	if record.Lease > 0 {
		if _, ok := ms.leases[record.Lease]; !ok {
			ms.lock.Unlock()
			return 0, kv.ErrWrongLeaseId
		}
	}

	record.Version = 0
	ms.data[record.Key] = record.Copy()
	ms.lock.Unlock()
	return record.Version, nil
}

// Get retrieves the record by its key. It will return nil and an error,
// which will indicate the reason why the operation was not succesful.
// ErrNotFound is returned if the key is not found in the storage
func (ms *mStorage) Get(ctx context.Context, key kv.Key) (kv.Record, error) {
	ms.lock.Lock()
	r, ok := ms.data[key]
	ms.lock.Unlock()
	if ok {
		return r.Copy(), nil
	}
	return kv.Record{}, kv.ErrNotFound
}

// GetRange returns the list of records for the range of keys (inclusively)
func (ms *mStorage) GetRange(ctx context.Context, startKey, endKey kv.Key) (kv.Records, error) {
	if startKey > endKey {
		return nil, nil
	}
	ms.lock.Lock()
	res := make(kv.Records, 0, 10)
	for k, r := range ms.data {
		if startKey <= k && k <= endKey {
			res = append(res, r.Copy())
		}
	}
	ms.lock.Unlock()
	return res, nil
}

// CasByVersion compares-and-sets the record Value if the record stored
// version is same as in the provided record. The record version will be updated
// too and returned in the result.
//
// an error will contain the reason if the operation was not successful, or
// the new version will be returned otherwise
func (ms *mStorage) CasByVersion(ctx context.Context, record kv.Record) (kv.Version, error) {
	err := kv.ErrNotFound
	ms.lock.Lock()
	if r, ok := ms.data[record.Key]; ok {
		if r.Version != record.Version {
			err = kv.ErrWrongVersion
		} else {
			err = nil
			record.Version++
			record.Lease = r.Lease
			ms.data[record.Key] = record.Copy()
			ms.notifyWaiters(record.Key)
		}
	}
	ms.lock.Unlock()
	return record.Version, err
}

// Delete removes the record from the storage by its key. It returns
// an error if the operation was not successful.
func (ms *mStorage) Delete(ctx context.Context, key kv.Key) error {
	ms.lock.Lock()
	err := kv.ErrNotFound
	if _, ok := ms.data[key]; ok {
		delete(ms.data, key)
		err = nil
		ms.notifyWaiters(key)
	}
	ms.lock.Unlock()
	return err
}

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
func (ms *mStorage) WaitForVersionChange(ctx context.Context, key kv.Key, version kv.Version) (kv.Record, error) {
	ms.lock.Lock()
	r, ok := ms.data[key]
	if !ok {
		ms.lock.Unlock()
		return kv.Record{}, kv.ErrNotFound
	}
	if r.Version != version {
		ms.lock.Unlock()
		return r.Copy(), nil
	}

	ch := ms.addWaiter(key)
	ms.lock.Unlock()

	select {
	case <-ctx.Done():
		ms.removeWaiter(key, ch)
		return kv.Record{}, ctx.Err()
	case <-ch:
		return ms.Get(ctx, key)
	}
}

func (ms *mStorage) addWaiter(key kv.Key) chan struct{} {
	ws, ok := ms.waiters[key]
	if !ok {
		ws = make([]chan struct{}, 0, 1)
	}
	ch := make(chan struct{})
	ws = append(ws, ch)
	ms.waiters[key] = ws
	return ch
}

func (ms *mStorage) notifyWaiters(key kv.Key) {
	ws, ok := ms.waiters[key]
	if !ok {
		return
	}
	delete(ms.waiters, key)
	for _, ch := range ws {
		close(ch)
	}
}

func (ms *mStorage) removeWaiter(key kv.Key, ch chan struct{}) {
	ms.lock.Lock()
	if ws, ok := ms.waiters[key]; ok {
		for i, w := range ws {
			if w == ch {
				ws[i] = ws[len(ws)-1]
				ws = ws[:len(ws)-1]
				if len(ws) == 0 {
					delete(ms.waiters, key)
				} else {
					ms.waiters[key] = ws
				}
				break
			}
		}
	}
	ms.lock.Unlock()
}

func (ms *mStorage) release(ls *lease) error {
	ms.lock.Lock()
	_, ok := ms.leases[ls.id]
	if !ok {
		ms.lock.Unlock()
		return kv.ErrNotFound
	}

	delete(ms.leases, ls.id)
	for k, r := range ms.data {
		if r.Lease == ls.id {
			delete(ms.data, k)
			ms.notifyWaiters(k)
		}
	}

	ms.lock.Unlock()
	return nil
}

// ------------------------------- Lessor -----------------------------------
func (ms *mStorage) NewLease(ctx context.Context, ttl time.Duration, keepAlive bool) (kv.Lease, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be positive, but it is %v", ttl)
	}
	ms.lock.Lock()
	ms.lsId++
	ls := new(lease)
	ls.id = ms.lsId
	ls.ms = ms
	ls.ttl = ttl
	if !keepAlive {
		ls.cch = make(chan struct{})
		ls.endTime = time.Now().Add(ttl)
		go func() {
			select {
			case <-time.After(ttl):
				ls.Release()
			case <-ls.cch:
			}
		}()
	}
	ms.leases[ls.id] = ls
	ms.lock.Unlock()
	return ls, nil
}

// GetLease returns a Lease object by it's Id
func (ms *mStorage) GetLease(lid kv.LeaseId) (kv.Lease, error) {
	ms.lock.Lock()
	ls, ok := ms.leases[lid]
	ms.lock.Unlock()
	if !ok {
		return nil, kv.ErrNotFound
	}
	return ls, nil
}

func (ls *lease) Id() kv.LeaseId {
	return ls.id
}

// TTL is time to the lease expiration
func (ls *lease) TTL() time.Duration {
	if !ls.endTime.IsZero() {
		return ls.endTime.Sub(time.Now())
	}
	return ls.ttl
}

// Release terminates (releases) the lease immediately
func (ls *lease) Release() error {
	return ls.ms.release(ls)
}
