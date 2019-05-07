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

package tmindex

import (
	"fmt"
	"github.com/logrange/range/pkg/bstorage"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"github.com/logrange/range/pkg/utils/errors"
	"sync"
)

type (
	ckindex struct {
		lock  sync.Mutex
		cond  *sync.Cond
		users int
		state int
		bks   *bstorage.Blocks
	}

	block struct {
		idx int
		buf []byte
		bks *bstorage.Blocks
	}

	record struct {
		ts  int64
		idx uint32
	}
)

var (
	errFullBlock  = fmt.Errorf("block is full")
	errAllMatches = fmt.Errorf("all values from the block matches the condition")
	errCorrupted  = fmt.Errorf("fatal error, storage is corrupted")
	emptyBlock    [blockSize]byte
)

const (
	hdrRecCountOffs = 0
	hdrLevelOffset  = 1
	hdrSize         = 20

	// recSize contains size of one record
	recSize = 12

	// maxRecsPerBlock number of records per block
	maxRecsPerBlock = 41

	// blockSize contains number of bytes in block
	blockSize = 512
)

const (
	stOk = iota
	stWaiting
	stWriting
	stClosed
)

var (
	stateNames = map[int]string{stOk: "Ok", stWaiting: "Waiting", stWriting: "Writing", stClosed: "Closed"}
)

// readBlock reads block by its index idx. If the block could not be read, the error is considered
// like fatal, so errCorrupted is returned
func readBlock(bks *bstorage.Blocks, idx int) (*block, error) {
	buf, err := bks.Block(idx)
	if err != nil {
		return nil, errCorrupted
	}

	return &block{idx, buf, bks}, nil
}

// newCkIndex creates new ckindex instance. Expects opened bks. It will take a control
// over the bks and closes it as needed.
func newCkIndex(bks *bstorage.Blocks) *ckindex {
	ci := new(ckindex)
	ci.cond = sync.NewCond(&ci.lock)
	ci.state = stOk
	ci.bks = bks
	return ci
}

// addRecord adds record r into the tree with the root block with index idx. If the idx < 0
// or the tree is full the new root block will be arranged and its index will be created and
// returned.
//
// NOTE: any error, except errors.ClosedState, must be considered like the storage corruption
// or the data inconsistency cause it can indicate of wrong index structure or corrupted tree.
// The invoking code of addRecord must take an action and re-build the index in case of such
// kind of errors.
func (ci *ckindex) addRecord(idx int, r record) (int, error) {
	if err := ci.acquireRead(); err != nil {
		return idx, err
	}

	if idx < 0 {
		var err error
		idx, err = ci.bks.ArrangeBlock()
		if err != nil {
			ci.releaseRead()
			return -1, err
		}
	}

	for {
		b, err := readBlock(ci.bks, idx)
		if err != nil {
			ci.releaseRead()
			return -1, err
		}

		err = b.addRecord(r)
		if err == errFullBlock {
			// the situation here that it is not possible to add record in the sub-tree
			// rooted by b. It needs to add new level, so arrange the new block, makes
			// it root and try to add data there.
			idx1, err := ci.bks.ArrangeBlock()
			if err != nil {
				ci.releaseRead()
				return -1, err
			}

			b1, err := readBlock(ci.bks, idx1)
			if err != nil {
				ci.releaseRead()
				return -1, err
			}
			b1.makeRootFor(b)
			idx = idx1
			continue
		}

		if err == nil {
			b, err := b.prune()
			if err == nil {
				idx = b.idx
			}
		}

		ci.releaseRead()
		return idx, err
	}
}

// deleteIndex deletes the whole tree addressedb by root block with index idx
func (ci *ckindex) deleteIndex(idx int) error {
	if err := ci.acquireRead(); err != nil {
		return err
	}

	b, err := readBlock(ci.bks, idx)
	if err != nil {
		ci.releaseRead()
		return err
	}

	err = b.free()
	ci.releaseRead()
	return err
}

// grEq returns the record, which timestamp is greater or equal to the
// provided one. If all records are less than provided ts, it returns THE LAST record(!)
func (ci *ckindex) grEq(idx int, ts int64) (record, error) {
	if err := ci.acquireRead(); err != nil {
		return record{}, err
	}

	r, err := ci.grEqInt(idx, ts)
	ci.releaseRead()
	return r, err
}

func (ci *ckindex) grEqInt(idx int, ts int64) (record, error) {
	b, err := readBlock(ci.bks, idx)
	if err != nil {
		return record{}, err
	}

	idx = b.findGrEqIdx(ts)
	r := b.readRecord(idx)
	if b.level() == 0 {
		return r, nil
	}

	return ci.grEqInt(int(r.idx), ts)
}

// less returns the record, which timestamp is less to the
// provided one. It returns NotFound if all known records has the ts greater or equal
// to the provided one
func (ci *ckindex) less(idx int, ts int64) (record, error) {
	if err := ci.acquireRead(); err != nil {
		return record{}, err
	}

	r, err := ci.lessInt(idx, ts)
	ci.releaseRead()
	return r, err
}

func (ci *ckindex) lessInt(idx int, ts int64) (record, error) {
	b, err := readBlock(ci.bks, idx)
	if err != nil {
		return record{}, err
	}

	idx = b.findLessIdx(ts)
	if idx == b.records() {
		return record{}, errAllMatches
	}

	r := b.readRecord(idx)
	if b.level() == 0 {
		return r, nil
	}

	return ci.lessInt(int(r.idx), ts)
}

// count returns number of records found by block rooted at idx.
func (ci *ckindex) count(idx int) (int, error) {
	if err := ci.acquireRead(); err != nil {
		return 0, err
	}

	cnt, err := ci.countInt(idx)
	ci.releaseRead()
	return cnt, err
}

func (ci *ckindex) countInt(idx int) (int, error) {
	b, err := readBlock(ci.bks, idx)
	if err != nil {
		return -1, err
	}

	if b.level() == 0 {
		return b.records(), nil
	}

	cnt := 0
	for i := 0; i < b.records(); i++ {
		r := b.readRecord(i)
		c, err := ci.countInt(int(r.idx))
		if err != nil {
			return -1, err
		}
		cnt += c
	}
	return cnt, nil
}

// acquireRead tells the ckindex that some go routine wants to make
// some read/write manipulations over the tree. This function is such of
// transactional mechanism, which gurantees tthat the storage will not be
// closed in the middle of read/write procedure.
// The function returns error if the acquision is not possible. It also can block
// the go routine for a short period of time if an exclusive access was
// acquired.
func (ci *ckindex) acquireRead() error {
	ci.lock.Lock()
	for {
		if ci.state == stOk {
			ci.users++
			ci.lock.Unlock()
			return nil
		}

		if ci.state == stClosed {
			ci.lock.Unlock()
			return errors.ClosedState
		}

		ci.cond.Wait()
	}
}

// releaseRead releases the multiple access, which must be acquired before
// the call.
func (ci *ckindex) releaseRead() {
	ci.lock.Lock()
	ci.users--
	if ci.state == stWaiting && ci.users == 0 {
		// if somebody waits an exclusive access
		ci.cond.Broadcast()
	}
	ci.lock.Unlock()
}

// Close closes the component. It will wait until all active users
// releases the ckindex if needed.
func (ci *ckindex) Close() error {
	ci.lock.Lock()
	for ci.users > 0 || (ci.state != stWaiting && ci.state != stOk) {
		if ci.state == stClosed {
			ci.lock.Unlock()
			return errors.ClosedState
		}

		ci.state = stWaiting
		ci.cond.Wait()
	}
	ci.state = stClosed
	ci.cond.Broadcast()
	ci.lock.Unlock()

	return ci.bks.Close()
}

// execExclusively allows to execute an external function fn, having exclusive
// access to the ci. The exclusive access guarantees that while fn is executed
// no other go routine can perform read/write actions to the index
func (ci *ckindex) execExclusively(fn func()) error {
	ci.lock.Lock()
	for ci.users > 0 || (ci.state != stWaiting && ci.state != stOk) {
		if ci.state == stClosed {
			ci.lock.Unlock()
			return errors.ClosedState
		}
		ci.state = stWaiting
		ci.cond.Wait()
	}
	ci.state = stWriting
	ci.lock.Unlock()

	defer func() {
		ci.lock.Lock()
		if ci.state == stWriting {
			ci.state = stOk
		}
		ci.cond.Broadcast()
		ci.lock.Unlock()
	}()

	fn()
	return nil
}

func (ci *ckindex) String() string {
	// NOTE not safe
	return fmt.Sprintf("ckindex: {users: %d, state: %s, bks: %s}", ci.users, stateNames[ci.state], ci.bks)
}

// makeRootFor makes the b root for br. b must be empty and just arranged.
func (b *block) makeRootFor(br *block) {
	b.buf[hdrRecCountOffs] = 1
	b.buf[hdrLevelOffset] = byte(b.level() + 1)
	rec := br.readRecord(0)
	b.writeRecord(0, record{rec.ts, uint32(br.idx)})
}

// free releases all underlying blocks and the block itself. If the function returns an error,
// the underlying storage should be considered inconsistent.
func (b *block) free() error {
	lvl := b.level()
	if lvl > 0 {
		recs := b.records()
		for i := 0; i < recs; i++ {
			r := b.readRecord(i)
			rb, err := readBlock(b.bks, int(r.idx))
			if err != nil {
				// fatal the storage could be inconsistent now
				return err
			}

			err = rb.free()
			if err != nil {
				return err
			}
		}
	}

	return b.freeThisOnly()
}

func (b *block) freeThisOnly() error {
	copy(b.buf, emptyBlock[:])
	err := b.bks.FreeBlock(b.idx)
	b.buf = nil
	b.bks = nil
	return err
}

// findInsertIdx check existing records and returns the index where the record must be inserted
func (b *block) findInsertIdx(ts int64) int {
	recs := b.records()
	if recs == 0 {
		return 0
	}

	i, j := 0, recs
	for i < j {
		h := int(uint(i+j) >> 1)
		r := b.readRecord(h)
		if r.ts < ts {
			i = h + 1
		} else {
			j = h
		}
	}

	return i
}

// findLessIdx check existing records and it returns the index of a bucket where the record search must
// be started from taking into the condition that b[h-1].ts < ts <= b[h]
//
// For example, for keys 40, 50, 60 (3 buckets with indexes 0, 1, 2), the search must return the following
// results:
// ts = 39 gives 0
// ts = 40 gives 0
// ts = 42 gives 1 (bucket with the key 50)
// ts = 50 gives 1
// ts = 63 gives 3 (number of buckets)
func (b *block) findLessIdx(ts int64) int {
	i, j := 0, b.records()
	for i < j {
		h := int(uint(i+j) >> 1)
		r := b.readRecord(h)
		if ts <= r.ts {
			if h == 0 {
				return 0
			}
			r1 := b.readRecord(h - 1)
			if r1.ts < ts {
				return h
			}
			j = h
		} else {
			i = h + 1
		}
	}
	return i
}

// findGrEqIdx allows to find the index at which bucket it needs to look for the position of ts
// The function allows to select left most neighbor for the ts.
//
// For example, for keys 40, 50, 60 (3 buckets with indexes 0, 1, 2), the search must return the following
// results:
// ts = 39 gives 0
// ts = 40 gives 0
// ts = 42 gives 0 (bucket with the key 40)
// ts = 50 gives 1
// ts = 63 gives 2 (last bucket)
func (b *block) findGrEqIdx(ts int64) int {
	n := b.records()
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		r := b.readRecord(h)
		if r.ts <= ts {
			if h == n-1 || r.ts == ts {
				return h
			}

			r1 := b.readRecord(h + 1)
			if r1.ts > ts {
				return h
			}
			i = h + 1
		} else {
			j = h
		}
	}

	return i
}

// removeLastRecord removes the last record. Returns an error if any
func (b *block) removeLastRecord() error {
	recs := b.records()
	if recs == 0 {
		return nil
	}

	if b.level() > 0 {
		r := b.readRecord(recs - 1)
		lb, err := readBlock(b.bks, int(r.idx))
		if err != nil {
			// corrupted
			return err
		}

		err = lb.free()
		if err != nil {
			return err
		}
	}
	b.buf[hdrRecCountOffs] = byte(recs - 1)
	return nil
}

// addRecord allows to place the record r into the block b.
func (b *block) addRecord(r record) error {
	var err error
	insIdx := b.findInsertIdx(r.ts)
	recs := b.records()

	// if insIdx is less than recs, it means that the record r position is less
	// than after the last one. We expect that any new record must be added at the
	// end, but here the ts order can be broken. To avoid ordering of the index position
	// we will drop all values at or after insIdx.
	for recs > insIdx {
		err = b.removeLastRecord()
		if err != nil {
			// unrecoverable.
			return err
		}
		recs--
	}

	lvl := b.level()
	if lvl == 0 {
		if recs == maxRecsPerBlock {
			return errFullBlock
		}
		b.writeRecord(recs, r)
		return nil
	}

	// if no records and level is not 0, will create new underlying block
	newBlock := recs == 0
	for {
		var lb *block
		if newBlock {
			bidx, err := b.bks.ArrangeBlock()
			if err != nil {
				// could not arrange a block. It is possible not fatal yet, so returns it as is
				return err
			}

			lb, err = readBlock(b.bks, bidx)
			if err != nil {
				// could not read just arranged block. Corrupted definitely.
				return err
			}

			lb.buf[hdrRecCountOffs] = 0
			lb.buf[hdrLevelOffset] = byte(lvl - 1)
			b.writeRecord(recs, record{r.ts, uint32(bidx)})
		} else {
			lastRec := b.readRecord(recs - 1)
			lb, err = readBlock(b.bks, int(lastRec.idx))
			if err != nil {
				// could not read an arranged block. Corrupted definitely.
				return err
			}
		}

		// now, try to add the record to the last block
		err = lb.addRecord(r)
		if err != errFullBlock {
			return err
		}

		// if it was last block, we cannot add a new one
		if recs == maxRecsPerBlock {
			return errFullBlock
		}

		// ok, last block is full, let's add new one.
		newBlock = true
	}
}

// prune allows to reduce the height of the tree. It frees the blocks and returns the new root
func (b *block) prune() (*block, error) {
	if b.level() == 0 || b.records() > 1 {
		return b, nil
	}

	r := b.readRecord(0)
	fb, err := readBlock(b.bks, int(r.idx))
	if err == nil {
		if b.freeThisOnly() != nil {
			return b, errCorrupted
		}

		return fb.prune()
	}

	return b, err
}

func (b *block) readRecord(ridx int) record {
	idx := hdrSize + ridx*recSize
	_, ts, _ := xbinary.UnmarshalUint64(b.buf[idx:])
	_, idx2, _ := xbinary.UnmarshalUint32(b.buf[idx+8:])
	return record{int64(ts), idx2}
}

func (b *block) writeRecord(ridx int, r record) {
	idx := hdrSize + ridx*recSize
	xbinary.MarshalUint64(uint64(r.ts), b.buf[idx:])
	xbinary.MarshalUint32(r.idx, b.buf[idx+8:])
	b.buf[hdrRecCountOffs] = byte(ridx + 1)
}

// records returns number of records in the block
func (b *block) records() int {
	return int(b.buf[hdrRecCountOffs])
}

func (b *block) level() int {
	return int(b.buf[hdrLevelOffset])
}
