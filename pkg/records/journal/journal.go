// journal package contains data structures and functions which allow to
// organize records storages into a database. The journaling database is organized
// for persisting records which are processed in FIFO order.
package journal

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/inmem"
)

type (
	Controller interface {
		GetOrCreate(ctx context.Context, jname string) (Journal, error)
	}

	Chunks          []records.Chunk
	ChnksController interface {
		// GetChunks returns known chunks for the journal sorted by their
		// chunk IDs
		GetChunks(jid uint64) Chunks
		NewChunk(ctx context.Context, jid uint64) (records.Chunk, error)
	}

	JPos struct {
		jid   uint64
		cid   uint64
		roffs int64
	}

	Journal interface {
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
	}

	journal struct {
		id     uint64
		name   string
		cc     ChnksController
		logger log4g.Logger
		lock   sync.Mutex

		wsem chan bool

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
			return n, JPos{j.id, c.Id(), offs}, err
		}

		if err == records.ErrMaxSizeReached {
			continue
		}
		break
	}

	return 0, JPos{}, err
}

func (j *journal) String() string {
	chnks := j.chunks.Load().(Chunks)
	return fmt.Sprintf("{id=%X, name=%d, chunks=%d}", j.id, j.name, len(chnks))
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
	// TODO add the chunk to the list
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
			return nil, nil //TODO - err not nil!!
		}

	}

	newChk := false
	chk := j.getLastChunk()
	if chk == nil || chk == prevC {
		chk, err = j.cc.NewChunk(ctx, j.id)
		newChk = err == nil
	}

	j.lock.Lock()
	// TODO chieck state here !!!
	if newChk {
		j.addNewChunk(chk)
	}

	j.wsem <- true
	j.lock.Unlock()
	return chk, err
}
