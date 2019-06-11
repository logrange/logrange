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
	"strings"
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

	// interval describes an time and pos interval with 2 points.
	// Invariant:
	// for block level > 0: p0.ts <= p1.ts, p1.idx is not used
	// for block level == 0: p0.ts <= p1.ts, p0.idx < p1.idx
	interval struct {
		p0, p1 record
	}
)

var (
	errFullBlock  = fmt.Errorf("block is full")
	errAllMatches = fmt.Errorf("all values from the block matches the condition")
	errNoMatches  = fmt.Errorf("no matches in the block")
	errCorrupted  = fmt.Errorf("fatal error, storage is corrupted")
	emptyBlock    [blockSize]byte
)

const (
	hdrRecCountOffs = 0
	hdrLevelOffset  = 1
	//hdrSize         = 20
	hdrSize = 2

	// rcSize contains the record structure size packed to 12 bytes
	recSize = 12

	// maxRecsPerBlock number of records per block
	//maxRecsPerBlock = 41
	maxRecsPerBlock      = 41
	maxIntervalsPerBlock = maxRecsPerBlock - 1

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
// NOTE: any error, except errors.ClosedState, must be considered like the storage currupted
// or the data inconsistency cause it can indicate of wrong index structure or corrupted tree.
// The invoking code of addRecord must take an action and re-build the index in case of such
// kind of errors.
func (ci *ckindex) addInterval(idx int, it interval) (int, error) {
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

		lr, err := b.addInterval(it)
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
			it.applyPrevRecord(lr)
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

// grEq returns the record with which position (idx) records with ts could
// be found. error could be errAllMatches what means any known interval matches
// the ts criteria, or noMatches which means no intervals which can contain
// records with ts
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

	intIdx := b.findIntervalIdx(ts)
	if intIdx < 0 {
		return record{}, errAllMatches
	}

	if intIdx == b.intervals() {
		return b.readLastRecordInTheTree(), nil
	}

	r := b.readRecord(intIdx)
	if b.level() == 0 {
		// returns p0 for the interval
		return r, nil
	}

	return ci.grEqInt(int(r.idx), ts)
}

// less returns the record with which position (idx) or less records with ts could
// be found. error could be errAllMatches what means any known interval matches
// the ts criteria, or noMatches which means no intervals which can contain
// records with ts
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

	intIdx := b.findIntervalIdx(ts)
	if intIdx < 0 {
		return b.readFirstRecordInTheTree(), nil
	}

	if intIdx == b.intervals() {
		return record{}, errAllMatches
	}

	r := b.readRecord(intIdx)
	if b.level() == 0 {
		// returns p1 for the interval
		return b.readRecord(intIdx + 1), nil
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
		return b.intervals(), nil
	}

	cnt := 0
	for i := 0; i < b.intervals(); i++ {
		r := b.readRecord(i)
		c, err := ci.countInt(int(r.idx))
		if err != nil {
			return -1, err
		}
		cnt += c
	}
	return cnt, nil
}

// taversal works over all records in ascending order and writes results into the res
func (ci *ckindex) traversal(root int, res []interval) ([]interval, error) {
	if err := ci.acquireRead(); err != nil {
		return nil, err
	}

	res, err := ci.traversalInt(root, res)
	ci.releaseRead()
	return res, err
}

func (ci *ckindex) traversalInt(root int, res []interval) ([]interval, error) {
	b, err := readBlock(ci.bks, root)
	if err != nil {
		return res, err
	}

	if b.level() == 0 {
		for i := 0; i < b.intervals(); i++ {
			p0 := b.readRecord(i)
			p1 := b.readRecord(i + 1)
			res = append(res, interval{p0, p1})
		}
		return res, nil
	}

	for i := 0; i < b.records()-1; i++ {
		r := b.readRecord(i)
		res, err = ci.traversalInt(int(r.idx), res)
		if err != nil {
			return res, err
		}
	}

	return res, nil
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

// WaitAllJobsDone closes the component. It will wait until all active users
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
// br must have at least 1 interval (2+ records)
func (b *block) makeRootFor(br *block) {
	b.buf[hdrRecCountOffs] = 0
	b.buf[hdrLevelOffset] = byte(br.level() + 1)
	b.setLastInterval(br.theBlockInterval())
}

// free releases all underlying blocks and the block itself. If the function returns an error,
// the underlying storage should be considered inconsistent.
func (b *block) free() error {
	lvl := b.level()

	if lvl > 0 {
		recs := b.records()
		for i := 0; i < recs-1; i++ {
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

func maxInt64(i1, i2 int64) int64 {
	if i1 > i2 {
		return i1
	}
	return i2
}

func minInt64(i1, i2 int64) int64 {
	if i1 < i2 {
		return i1
	}
	return i2
}

func maxUint32(i1, i2 uint32) uint32 {
	if i1 > i2 {
		return i1
	}
	return i2
}

func minUint32(i1, i2 uint32) uint32 {
	if i1 < i2 {
		return i1
	}
	return i2
}

func maxInt(i1, i2 int) int {
	if i1 > i2 {
		return i1
	}
	return i2
}

// readFirstRecordInTheTree reads first record in the tree where b is the root. block must not be empty
func (b *block) readFirstRecordInTheTree() record {
	if b.records() == 0 {
		return record{}
	}

	r := b.readRecord(0)
	if b.level() == 0 {
		return r
	}

	if bl, err := readBlock(b.bks, int(r.idx)); err == nil {
		return bl.readFirstRecordInTheTree()
	}

	return record{}
}

// readLastRecordInTheTree reads first record in the tree where b is the root. block must not be empty
func (b *block) readLastRecordInTheTree() record {
	rcrds := b.records()
	if rcrds == 0 {
		return record{}
	}

	if b.level() == 0 {
		return b.readRecord(rcrds - 1)
	}

	r := b.readRecord(rcrds - 2)
	if bl, err := readBlock(b.bks, int(r.idx)); err == nil {
		return bl.readLastRecordInTheTree()
	}

	return record{}
}

// findIntervalInsertIdx returns the interval index where ts can be added
// the result is in [-1 .. b.intervals()] the result == b.intervals() means
// the interval must be added to the end. -1 means the ts comes before any
// known interval ts
func (b *block) findIntervalInsertIdx(ts int64) int {
	recs := b.records()
	if recs == 0 {
		return 0
	}

	i, j := 0, recs
	for i < j {
		h := int(uint(i+j) >> 1)
		r := b.readRecord(h)
		if r.ts <= ts {
			i = h + 1
		} else {
			j = h
		}
	}

	if b.level() == 0 {
		// for level 0 returns -1 .. intervals()
		return i - 1
	}

	if i == recs {
		// for blocks with level > 0, let's return last index, for extension
		return recs - 2
	}

	return maxInt(0, i-1)
}

// findIntervalIdx returns interval index where ts falls into. The
// result is in [-1 .. b.intervals()] where -1 and b.intervals() means
// that ts falls out of the known intervals range
func (b *block) findIntervalIdx(ts int64) int {
	recs := b.records()
	if recs == 0 {
		return 0
	}

	i, j := 0, recs
	for i < j {
		h := int(uint(i+j) >> 1)
		r := b.readRecord(h)
		if r.ts <= ts {
			i = h + 1
		} else {
			j = h
		}
	}

	return i - 1
}

func (b *block) removeLastInterval() error {
	recs := b.records()
	if recs == 0 {
		return nil
	}

	if b.level() > 0 {
		// for upper levels, remove underlying blocks
		r := b.readRecord(recs - 2)
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

	if recs == 2 {
		// this is firs interval, so will put 0 records
		recs = 1
	}
	b.buf[hdrRecCountOffs] = byte(recs - 1)

	return nil
}

func (b *block) theBlockInterval() interval {
	recs := b.records()
	if recs == 0 {
		panic("must not be invoked with 0 records")
	}

	fr := b.readRecord(0)
	lr := b.readRecord(recs - 1)
	fr.idx = uint32(b.idx)
	return interval{fr, lr}
}

func (b *block) setLastInterval(it interval) {
	recs := b.records()
	if recs == 0 {
		b.writeRecord(0, it.p0)
		b.writeRecord(1, it.p1)
		return
	}
	b.writeRecord(recs-2, it.p0)
	b.writeRecord(recs-1, it.p1)
}

func (b *block) appendInterval(it *interval) (record, error) {
	recs := b.records()
	if recs == maxRecsPerBlock {
		return b.readRecord(recs - 1), errFullBlock
	}

	if recs == 0 {
		b.writeRecord(0, it.p0)
		b.writeRecord(1, it.p1)
		return it.p1, nil
	}
	b.writeRecord(recs, it.p1)
	return it.p1, nil
}

// addInterval allows to place the interval it into the block b.
// it returns last record on the level 0, which has a valid value even if error == errFullBlock
func (b *block) addInterval(it interval) (record, error) {
	var err error
	insIdx := b.findIntervalInsertIdx(it.p0.ts)
	ints := b.intervals()

	lvl := b.level()
	if lvl == 0 {
		if insIdx == ints {
			return b.appendInterval(&it)
		}

		if ints > 0 {
			lastRec := b.readRecord(ints)
			it.p1.ts = maxInt64(it.p1.ts, lastRec.ts)
		}

		if insIdx < 0 {
			if ints > 0 {
				firstRec := b.readRecord(0)
				it.p0 = it.p0.reduce(firstRec)
			}
			b.buf[hdrRecCountOffs] = 0
		} else {
			it.p0 = b.readRecord(insIdx)
			// set the records number, so insIdx is the last one
			b.buf[hdrRecCountOffs] = byte(insIdx + 2)
		}
		b.setLastInterval(it)
		return it.p1, nil
	}

	// ok we are not on 0 level, so let's try to extend or insert
	// first, let's remove all intervals that go after insIdx
	for i := insIdx + 1; i < ints; i++ {
		if err := b.removeLastInterval(); err != nil {
			// unrecoverable, so empty record...
			return record{}, err
		}
	}
	ints = b.intervals()

	// if no records let's create new underlying block
	newBlock := ints == 0
	for {
		var lb *block
		if newBlock {
			bidx, err := b.bks.ArrangeBlock()
			if err != nil {
				// could not arrange a block. It is possible not fatal yet, so returns it as is
				return record{}, err
			}

			lb, err = readBlock(b.bks, bidx)
			if err != nil {
				// could not read just arranged block. Corrupted definitely.
				return record{}, err
			}

			lb.buf[hdrRecCountOffs] = 0
			lb.buf[hdrLevelOffset] = byte(lvl - 1)
		} else {
			lastInt := b.readRecord(insIdx)
			lb, err = readBlock(b.bks, int(lastInt.idx))
			if err != nil {
				// could not read an arranged block. Corrupted definitely.
				return record{}, err
			}
		}

		// now, try to add the record to the last block
		lr, err := lb.addInterval(it)
		if err == nil {
			b.setLastInterval(lb.theBlockInterval())
			return lr, nil
		}

		if err != errFullBlock {
			if newBlock {
				_ = lb.freeThisOnly()
			}
			// corrupted for sure
			return lr, err
		}

		// ok, last block is full, can we add more?
		if _, err = b.appendInterval(&it); err != nil {
			return lr, errFullBlock
		}

		// no spaces between interval blocks
		it.applyPrevRecord(lr)
		newBlock = true
	}
}

// freeThisOnly makes the block free
func (b *block) freeThisOnly() error {
	copy(b.buf, emptyBlock[:])
	err := b.bks.FreeBlock(b.idx)
	b.buf = nil
	b.bks = nil
	return err
}

// prune allows to reduce the height of the tree. It frees the blocks with high level,
// but which have only one interval with its child. The method returns the new root
// or an error, if any.
func (b *block) prune() (*block, error) {
	if b.level() == 0 || b.intervals() > 1 {
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

// readInterval read the interval with index iidx
func (b *block) readInterval(iidx int) interval {
	return interval{b.readRecord(iidx), b.readRecord(iidx + 1)}
}

// readRecord allows to read record at ridx
func (b *block) readRecord(ridx int) record {
	idx := hdrSize + ridx*recSize
	_, ts, _ := xbinary.UnmarshalUint64(b.buf[idx:])
	_, idx2, _ := xbinary.UnmarshalUint32(b.buf[idx+8:])
	return record{int64(ts), idx2}
}

// writeRecord allows to write record by index ridx
func (b *block) writeRecord(ridx int, r record) {
	idx := hdrSize + ridx*recSize
	_, _ = xbinary.MarshalUint64(uint64(r.ts), b.buf[idx:])
	_, _ = xbinary.MarshalUint32(r.idx, b.buf[idx+8:])
	b.buf[hdrRecCountOffs] = byte(ridx + 1)
}

// records returns number of records in the block
func (b *block) records() int {
	return int(b.buf[hdrRecCountOffs])
}

// intervals returns number of intervals in the block
func (b *block) intervals() int {
	recs := int(b.buf[hdrRecCountOffs])
	if recs <= 1 {
		return 0
	}
	return recs - 1
}

func (b *block) String() string {
	var sb strings.Builder
	recs := b.records()
	sb.WriteString(fmt.Sprintf("{block idx=%d, level=%d records=%d ", b.idx, b.level(), recs))
	for i := 0; i < recs; i++ {
		r := b.readRecord(i)
		sb.WriteString(fmt.Sprintf("{%d : %d }", r.ts, r.idx))
	}
	return sb.String()
}

func (b *block) level() int {
	return int(b.buf[hdrLevelOffset])
}

func (i *interval) applyPrevRecord(r record) {
	i.p0 = r
}

func (r record) extend(r1 record) record {
	return record{maxInt64(r.ts, r1.ts), maxUint32(r.idx, r1.idx)}
}

func (r record) reduce(r1 record) record {
	return record{minInt64(r.ts, r1.ts), minUint32(r.idx, r1.idx)}
}
