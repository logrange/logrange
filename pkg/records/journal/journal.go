// journal package contains data structures and functions which allow to
// organize records storages into a database. The journaling database is organized
// for persisting records which are processed in FIFO order.
package journal

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/util"
)

type (
	Controller interface {
		GetOrCreate(ctx context.Context, jname string) (Journal, error)
	}

	ChnksController interface {
		// GetChunks returns known chunks for the journal sorted by their
		// chunk IDs. The function returns non-nil Chunks slice, which
		// always has at least one chunk. If the journal has just been
		// created the ChnksController creates new chunk for it.
		GetChunks(jid uint64) chunk.Chunks
		NewChunk(ctx context.Context, jid uint64) (chunk.Chunk, error)
	}

	Pos struct {
		CId  uint64
		Offs int64
	}

	Journal interface {
		io.Closer
		// Write - writes records received from the iterator to the journal.
		// It returns number of records written, first record position and an error if any
		Write(ctx context.Context, rit records.Iterator) (int, Pos, error)

		// Size returns the summarized chunks size
		Size() int64

		// Iterator returns an iterator to walk through the journal records
		Iterator() (Iterator, error)
	}

	Iterator interface {
		io.Closer
		records.Iterator

		Pos() Pos
		Reset(pos Pos, bkwd bool)
	}

	journal struct {
		id     uint64
		name   string
		cc     ChnksController
		logger log4g.Logger
		lock   sync.Mutex
		cond   *sync.Cond

		// data waiters, the goroutines which waits till a new data appears
		waiters int32
		dwChnls []chan bool

		// wsem is for creating new chunks while write
		wsem chan bool

		// closed the journal state flag
		closed bool

		// contains []records.Chunk in sorted order
		chunks atomic.Value

		// settings
		maxChunkSize uint64
	}
)

// JidFromName returns a journal id (jid) by its name
func JidFromName(jname string) uint64 {
	ra := sha1.Sum(records.StringToByteArray(jname))
	return (uint64(ra[7]) << 56) | (uint64(ra[6]) << 48) | (uint64(ra[5]) << 40) |
		(uint64(ra[4]) << 32) | (uint64(ra[3]) << 24) | (uint64(ra[2]) << 16) |
		(uint64(ra[1]) << 8) | uint64(ra[0])
}

func New(cc ChnksController, jname string) (Journal, error) {
	j := new(journal)
	j.id = JidFromName(jname)
	j.name = jname
	j.cc = cc
	j.cond = sync.NewCond(&j.lock)
	j.chunks.Store(cc.GetChunks(j.id))
	j.wsem = make(chan bool, 1)
	j.wsem <- true
	j.logger = log4g.GetLogger("journal").WithId("{" + j.name + ":" + strconv.FormatUint(j.id, 16) + "}").(log4g.Logger)
	j.logger.Info("New instance created ", j)
	return j, nil
}

// Write - writes records received from the iterator to the journal.
func (j *journal) Write(ctx context.Context, rit records.Iterator) (int, Pos, error) {
	var err error
	var c chunk.Chunk
	for _, err = rit.Get(ctx); err == nil; {
		c, err = j.getWriterChunk(ctx, c)
		if err != nil {
			return 0, Pos{}, err
		}

		n, offs, err := c.Write(ctx, rit)
		if n > 0 {
			return n, Pos{c.Id(), offs}, err
		}

		if err == util.ErrMaxSizeReached {
			continue
		}
		break
	}

	return 0, Pos{}, err
}

func (j *journal) Iterator() (Iterator, error) {
	return nil, nil
}

func (j *journal) Size() int64 {
	chunks := j.chunks.Load().(chunk.Chunks)
	var sz int64
	for _, c := range chunks {
		sz += c.Size()
	}
	return sz
}

func (j *journal) Close() error {
	j.lock.Lock()
	defer j.lock.Unlock()

	if j.closed {
		j.logger.Warn("Journal is already closed, ignoring this Close() call ", j)
		return nil
	}

	j.logger.Info("Closing the journal ", j)
	j.closed = true

	close(j.wsem)
	j.chunks.Store(make(chunk.Chunks, 0, 0))

	return nil
}

func (j *journal) String() string {
	chnks := j.chunks.Load().(chunk.Chunks)
	return fmt.Sprintf("{id=%X, name=%d, chunks=%d, closed=%t}", j.id, j.name, len(chnks), j.closed)
}

func (j *journal) getLastChunk() chunk.Chunk {
	chunks := j.chunks.Load().(chunk.Chunks)
	if len(chunks) == 0 {
		j.logger.Debug("getLastChunk: chunks are empty")
		return nil
	}
	return chunks[len(chunks)-1]
}

func (j *journal) addNewChunk(c chunk.Chunk) {
	chnks := j.chunks.Load().(chunk.Chunks)
	newChnks := make(chunk.Chunks, len(chnks)+1)
	copy(newChnks, chnks)
	newChnks[len(newChnks)-1] = c
	j.chunks.Store(chunk.Chunks(newChnks))
}

func (j *journal) getWriterChunk(ctx context.Context, prevC chunk.Chunk) (chunk.Chunk, error) {
	if prevC == nil {
		prevC = j.getLastChunk()
		if prevC != nil {
			return prevC, nil
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case _, ok := <-j.wsem:
		if !ok {
			// the journal is closed
			return nil, util.ErrWrongState
		}
	}

	var err error
	newChk := false
	chk := j.getLastChunk()
	if chk == nil || chk == prevC {
		chk, err = j.cc.NewChunk(ctx, j.id)
		newChk = err == nil
	}

	j.lock.Lock()
	if !j.closed {
		if newChk {
			j.logger.Debug("New chunk is created ", chk)
			j.addNewChunk(chk)
		}

		j.wsem <- true
	} else if err == nil {
		// if the chunk was created, but the journal is already closed
		err = util.ErrWrongState
	}
	j.lock.Unlock()
	return chk, err
}

// Called by chunk writer when synced
func (j *journal) whenDataWritten() {
	if atomic.LoadInt32(&j.waiters) <= 0 {
		return
	}

	j.lock.Lock()
	defer j.lock.Unlock()

	for i, ch := range j.dwChnls {
		close(ch)
		j.dwChnls[i] = nil
	}

	if cap(j.dwChnls) > 10 {
		j.dwChnls = nil
	} else {
		j.dwChnls = j.dwChnls[:0]
	}
}

// waitData allows to wait a new data is written into the journal. The function
// expects ctx context and curId a record Id to check the operation against to.
// The call will block current go routine, if the curId lies behind the last
// record in the journal. The case, the goroutine will be unblock until one of the
// two events happen - ctx is closed or new records added to the journal.
func (j *journal) waitData(ctx context.Context, curId Pos) error {
	atomic.AddInt32(&j.waiters, 1)
	defer atomic.AddInt32(&j.waiters, -1)

	for {
		j.lock.Lock()
		if j.closed {
			j.lock.Unlock()
			return util.ErrWrongState
		}

		chk := j.getLastChunk()
		lro := Pos{chk.Id(), chk.GetLastRecordOffset()}
		if curId.CmpLE(lro) {
			j.lock.Unlock()
			return nil
		}
		curId = lro
		curId.Offs++

		ch := make(chan bool)
		if j.dwChnls == nil {
			j.dwChnls = make([]chan bool, 0, 10)
		}
		j.dwChnls = append(j.dwChnls, ch)
		j.lock.Unlock()

		select {
		case <-ctx.Done():
			j.lock.Lock()
			ln := len(j.dwChnls)
			for i, c := range j.dwChnls {
				if c == ch {
					j.dwChnls[i] = j.dwChnls[ln-1]
					j.dwChnls[ln-1] = nil
					j.dwChnls = j.dwChnls[:ln-1]
					close(ch)
					break
				}
			}
			j.lock.Unlock()
			return ctx.Err()
		case <-ch:
			// the select
			break
		}
	}
}

// getChunkById is looking for a chunk with cid. If there is no such chunk
// in the list, it will return the chunk which is most left in case of bkwd=true
// or most right otherwise. If there is no such chunks, so cid points is out
// of the chunks range, then the method return nil
func (j *journal) getChunkById(cid uint64, bkwd bool) chunk.Chunk {
	chunks := j.chunks.Load().(chunk.Chunks)
	n := len(chunks)
	if n == 0 {
		return nil
	}

	idx := sort.Search(len(chunks), func(i int) bool { return chunks[i].Id() >= cid })
	// according to the condition idx is always in [0..n]
	if bkwd && (idx == n || chunks[idx].Id() > cid) {
		idx--
	}

	if idx < 0 || idx >= n {
		return nil
	}
	return chunks[idx]
}

// ---------------------------- JournalPos -----------------------------------
func (jp Pos) String() string {
	return fmt.Sprintf("{CId=%X, Offs=%d}", jp.CId, jp.Offs)
}

func (jp Pos) CmpLE(jp2 Pos) bool {
	return jp.CId < jp2.CId || (jp.CId == jp2.CId && jp.Offs <= jp2.Offs)
}
