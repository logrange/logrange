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

package inmem

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/logrange/logrange/pkg/kv"
)

func TestCreate(t *testing.T) {
	ms := New()

	var r kv.Record
	_, err := ms.Create(nil, r)
	if err == nil || len(ms.data) != 0 {
		t.Fatal("must be an error, but seems that err==nil")
	}

	r.Key = "aaa"
	_, err = ms.Create(nil, r)
	if err != nil || len(ms.data) != 1 {
		t.Fatal("must be no error, but err=", err)
	}

	_, err = ms.Create(nil, r)
	if err != kv.ErrAlreadyExists {
		t.Fatal("err must be ErrAlreadyExists, but err=", err)
	}

	r.Key = "ddd"
	r.Lease = 123
	_, err = ms.Create(nil, r)
	if err != kv.ErrWrongLeaseId {
		t.Fatal("err must be ErrWrongLeaseId, but err=", err)
	}

	ls, err := ms.Lessor().NewLease(nil, 1, true)
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}
	r.Lease = ls.Id()
	_, err = ms.Create(nil, r)
	if err != nil || len(ms.data) != 2 {
		t.Fatal("must be no error, but err=", err)
	}
}

func TestGet(t *testing.T) {
	ms := New()

	_, err := ms.Get(nil, "aaa")
	if err != kv.ErrNotFound {
		t.Fatal("err must be ErrNotFound, but err=", err)
	}

	var r kv.Record
	r.Key = "aaa"
	r.Value = []byte{1, 2, 3}
	ms.Create(nil, r)

	r1, err := ms.Get(nil, r.Key)
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}
	if string(r.Value) != string(r1.Value) || r.Key != r1.Key {
		t.Fatal("stored ", r, ", but read ", r1)
	}

	r.Value[0] = 22
	r1, _ = ms.Get(nil, r.Key)
	if r1.Value[0] != 1 {
		t.Fatal("Expecting container immutability")
	}
}

func TestGetRange(t *testing.T) {
	ms := New()

	newTestRecord(ms, "a/aaa")
	newTestRecord(ms, "a/a")
	newTestRecord(ms, "a/abb")
	newTestRecord(ms, "aa/abb")
	newTestRecord(ms, "b/abb")

	rcs, err := ms.GetRange(nil, "a/", "a0")
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}
	testRecords(t, rcs, []string{"a/a", "a/aaa", "a/abb"})

	rcs, _ = ms.GetRange(nil, "a/aa", "a/ac")
	testRecords(t, rcs, []string{"a/aaa", "a/abb"})

	rcs, err = ms.GetRange(nil, "a/ac", "a/aa")
	if len(rcs) != 0 || err != nil {
		t.Fatal("Expecting 0 result but ", rcs)
	}
}

func TestCasByVersion(t *testing.T) {
	ms := New()

	ver, err := newTestRecord(ms, "aaa")
	r, _ := ms.Get(nil, "aaa")
	if r.Version != ver {
		t.Fatal("Expected ver=", ver, ", but actual is ", r.Version)
	}

	r.Value = []byte("bbb")
	v2, err := ms.CasByVersion(nil, r)
	if v2 == r.Version || err != nil {
		t.Fatal("Wrong v2=", v2, " should not be ", r, ", err=", err)
	}

	r2, _ := ms.Get(nil, "aaa")
	if string(r2.Value) != string(r.Value) {
		t.Fatal("Must be updated version, but ", r2, " initial ", r)
	}

	_, err = ms.CasByVersion(nil, r)
	if err != kv.ErrWrongVersion {
		t.Fatal("Must be wrong version for the case")
	}

	r2, _ = ms.Get(nil, "aaa")
	if string(r2.Value) != string(r.Value) || r2.Version != v2 {
		t.Fatal("Must be updated version, but ", r2, " initial ", r)
	}
}

func TestDelete(t *testing.T) {
	ms := New()
	newTestRecord(ms, "aaa")
	err := ms.Delete(nil, "aaa")
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}

	err = ms.Delete(nil, "aaa")
	if err != kv.ErrNotFound {
		t.Fatal("must be ErrNotFound error, but err=", err)
	}
}

func TestWaitForVersionChange(t *testing.T) {
	ms := New()
	rec, err := ms.WaitForVersionChange(context.Background(), "a", 0)
	if err != kv.ErrNotFound {
		t.Fatal("must be not found record, but err=", err)
	}

	ver, _ := newTestRecord(ms, "a")
	rec, err = ms.WaitForVersionChange(context.Background(), "a", ver-1)
	if err != nil || rec.Key != "a" {
		t.Fatal("must be found immediately, but err=", err, ", rec=", rec)
	}

	// update
	start := time.Now()
	go func(r kv.Record) {
		time.Sleep(5 * time.Millisecond)
		r.Value = []byte("b")
		_, err := ms.CasByVersion(nil, r)
		if err != nil {
			panic("Cas failed err=" + err.Error())
		}
	}(rec)
	rec, err = ms.WaitForVersionChange(context.Background(), "a", ver)
	if time.Now().Sub(start) < 5*time.Microsecond {
		t.Fatal("expecting at least 5 ms delay")
	}
	if err != nil || rec.Key != "a" || string(rec.Value) != "b" {
		t.Fatal("something wrong, err=", err, ", rec=", rec)
	}

	// context closed
	cctx, cancel := context.WithCancel(context.Background())
	start = time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	_, err = ms.WaitForVersionChange(cctx, "a", rec.Version)
	if time.Now().Sub(start) < 5*time.Microsecond {
		t.Fatal("expecting at least 5 ms delay")
	}
	if err != cctx.Err() {
		t.Fatal("must be reported the context error, but err=", err)
	}

	// record deleted
	start = time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		ms.Delete(nil, rec.Key)
	}()
	_, err = ms.WaitForVersionChange(context.Background(), "a", rec.Version)
	if time.Now().Sub(start) < 5*time.Microsecond {
		t.Fatal("expecting at least 5 ms delay")
	}
	if err != kv.ErrNotFound {
		t.Fatal("must be reported the context error, but err=", err)
	}
}

func TestKeepAliveLease(t *testing.T) {
	ms := New()
	ls, err := ms.NewLease(nil, time.Millisecond, true)
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}

	r := kv.Record{Key: "aaa", Value: []byte("bbb"), Lease: ls.Id()}
	ms.Create(nil, r)

	time.Sleep(5 * time.Millisecond)
	if ls.TTL() != time.Millisecond {
		t.Fatal("This lease is keep alive forever, must be 1ms, but ", ls.TTL())
	}

	_, err = ms.Get(nil, "aaa")
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}

	newTestRecord(ms, "aa")
	rcs, _ := ms.GetRange(nil, "a", "b")
	testRecords(t, rcs, []string{"aa", "aaa"})

	ls.Release()

	_, err = ms.Get(nil, "aaa")
	if err != kv.ErrNotFound {
		t.Fatal("must be ErrNotFound, but err=", err)
	}

	rcs, _ = ms.GetRange(nil, "a", "b")
	testRecords(t, rcs, []string{"aa"})
}

func TestLease(t *testing.T) {
	ms := New()
	ls, err := ms.NewLease(nil, time.Millisecond, false)
	if err != nil {
		t.Fatal("must be no error, but err=", err)
	}

	r := kv.Record{Key: "aaa", Value: []byte("bbb"), Lease: ls.Id()}
	ms.Create(nil, r)
	r, err = ms.Get(nil, "aaa")
	if err != nil || r.Lease != ls.Id() {
		t.Fatal("must be no error, but err=", err, " r=", r)
	}

	time.Sleep(5 * time.Millisecond)

	_, err = ms.Get(nil, "aaa")
	if err != kv.ErrNotFound || len(ms.data) != 0 || len(ms.leases) != 0 {
		t.Fatal("must be ErrNotFound, but err=", err, " ms=", ms)
	}
}

func testRecords(t *testing.T, rcs kv.Records, vals []string) {
	if len(vals) != len(rcs) {
		t.Fatal("Expecting ", len(vals), " records, but received ", len(rcs), "vals=", vals, " rcs=", rcs)
	}

	sort.Sort(rcs)
	for i, r := range rcs {
		if string(r.Key) != vals[i] {
			t.Fatal("Expecting ", vals[i], " at position ", i, ", but got ", r.Key)
		}
	}
}

func newTestRecord(ms *mStorage, val string) (kv.Version, error) {
	r := kv.Record{Key: kv.Key(val), Value: []byte(val)}
	return ms.Create(nil, r)
}
