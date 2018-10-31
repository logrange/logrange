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
	"github.com/logrange/logrange/pkg/records/inmem"
	"github.com/logrange/logrange/pkg/util"
)

type (
	Controller interface {
		GetOrCreate(ctx context.Context, jname string) (Journal, error)
	}

	Chunks          []records.Chunk
	ChnksController interface {
		// GetChunks returns known chunks for the journal sorted by their
		// chunk IDs. The function returns non-nil Chunks slice, which
		// always has at least one chunk. If the journal has just been
		// created the ChnksController creates new chunk for it.
		GetChunks(jid uint64) Chunks
		NewChunk(ctx context.Context, jid uint64) (records.Chunk, error)
	}

	JPos struct {
		cid   uint64
		roffs int64
	}

	Journal interface {
		io.Closer
		// Write - writes records received from the iterator to the journal.
		// It returns number of records written, first record position and an error if any
		Write(ctx context.Context, rit records.Iterator) (int, JPos, error)

		// Size returns the summarized chunks size
		Size() int64

		// Iterator returns an iterator to walk through the journal records
		Iterator() (JournalIterator, error)
	}

	JournalIterator interface {
		io.Closer
		records.Iterator

		Pos() JPos
		SetPos(pos JPos)
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
	ra := sha1.Sum(inmem.StringToByteArray(jname))
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
func (j *journal) Write(ctx context.Context, rit records.Iterator) (int, JPos, error) {
	var err error
	var c records.Chunk
	for _, err = rit.GetCtx(ctx); err == nil; {
		c, err = j.getWriterChunk(ctx, c)
		if err != nil {
			return 0, JPos{}, err
		}

		n, offs, err := c.Write(ctx, rit)
		if n > 0 {
			return n, JPos{c.Id(), offs}, err
		}

		if err == util.ErrMaxSizeReached {
			continue
		}
		break
	}

	return 0, JPos{}, err
}

func (j *journal) Iterator() (JournalIterator, error) {
	return nil, nil
}

func (j *journal) Size() int64 {
	chunks := j.chunks.Load().(Chunks)
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
	j.chunks.Store(make(Chunks, 0, 0))

	return nil
}

func (j *journal) String() string {
	chnks := j.chunks.Load().(Chunks)
	return fmt.Sprintf("{id=%X, name=%d, chunks=%d, closed=%t}", j.id, j.name, len(chnks), j.closed)
}

func (j *journal) getLastChunk() records.Chunk {
	chunks := j.chunks.Load().(Chunks)
	if len(chunks) == 0 {
		j.logger.Debug("getLastChunk: chunks are empty")
		return nil
	}
	return chunks[len(chunks)-1]
}

func (j *journal) addNewChunk(c records.Chunk) {
	chnks := j.chunks.Load().(Chunks)
	newChnks := make(Chunks, len(chnks)+1)
	copy(newChnks, chnks)
	newChnks[len(newChnks)-1] = c
	j.chunks.Store(Chunks(newChnks))
}

func (j *journal) getWriterChunk(ctx context.Context, prevC records.Chunk) (records.Chunk, error) {
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
func (j *journal) waitData(ctx context.Context, curId JPos) error {
	atomic.AddInt32(&j.waiters, 1)
	defer atomic.AddInt32(&j.waiters, -1)

	for {
		j.lock.Lock()
		if j.closed {
			j.lock.Unlock()
			return util.ErrWrongState
		}

		chk := j.getLastChunk()
		lro := JPos{chk.Id(), chk.GetLastRecordOffset()}
		if curId.CmpLE(lro) {
			j.lock.Unlock()
			return nil
		}
		curId = lro
		curId.roffs++

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

func (j *journal) getChunkById(cid uint64) records.Chunk {
	chunks := j.chunks.Load().(Chunks)
	n := len(chunks)
	if n == 0 {
		return nil
	}

	if chunks[0].Id() >= cid {
		return chunks[0]
	}

	if chunks[n-1].Id() < cid {
		return chunks[n-1]
	}

	return chunks[sort.Search(len(chunks), func(i int) bool { return chunks[i].Id() >= cid })]
}

// ------------------------------- JPos --------------------------------------
func (jp JPos) String() string {
	return fmt.Sprintf("{cid=%X, roffs=%d}", jp.cid, jp.roffs)
}

func (jp JPos) CmpLE(jp2 JPos) bool {
	return jp.cid < jp2.cid || (jp.cid == jp2.cid && jp.roffs <= jp2.roffs)
}
