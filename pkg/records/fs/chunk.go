package fs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
)

var (
	ErrWrongOffset    = fmt.Errorf("Error wrong offset while seeking")
	ErrWrongState     = fmt.Errorf("Wrong state, probably already closed.")
	ErrBufferTooSmall = fmt.Errorf("Too small buffer for read")
	ErrCorruptedData  = fmt.Errorf("Chunk data has an inconsistency in the data inside")
)

type (
	ChunkConfig struct {
		FileName       string
		ChnkCheckFlags int
		WriteIdleSec   int
		WriteFlushMs   int
		MaxChunkSize   int64
	}

	// Chunk struct allows to control read and write operations over the underlying
	// data file
	Chunk struct {
		fileName string

		lock   sync.Mutex
		fdPool *FdPool
		closed bool
		logger log4g.Logger

		// writing part
		w *cWrtier
	}

	// ChunkIterator struct provides records.Iterator interface for accessing
	// to the chunk data in an ascending order. It must be used from only one
	// go-routine at a time (not thread safe)
	ChunkIterator struct {
	}
)

const (
	ChnkDataHeaderSize  = 4
	ChnkDataRecMetaSize = ChnkDataHeaderSize * 2
	ChnkWriterIdleTO    = time.Minute
	ChnkWriterFlushTO   = 500 * time.Millisecond

	ChnkWriterBufSize = 4 * 4096
	ChnkReaderBufSize = 2 * 4096

	// ChnkChckTruncateOk means that chunk data can be truncated to good size
	ChnkChckTruncateOk = 1

	// ChnkChckFullScan means that full scan to find last record must be performed
	ChnkChckFullScan = 2
)

// NewChunk returns new chunk using the following params:
// ctx - is used to control the creation of the chunk process. If the underlying
// 		chunk file is not empty, a checker, which will use fReader will be involved
//		the checker can take significant time due to lack of read descriptors or
// 		by the process itself
// cfg - contains configuration settings for the chunk
// fdPool - file descriptors pool, which is used for holding fdReader objects.
func NewChunk(ctx context.Context, cfg *ChunkConfig, fdPool *FdPool) (*Chunk, error) {
	// run the checker
	lid := "{" + cfg.FileName + "}"
	chkr := &cChecker{fileName: cfg.FileName, fdPool: fdPool, logger: log4g.GetLogger("chunk.checker").WithId(lid).(log4g.Logger)}
	err := chkr.checkFileConsistency(ctx, cfg.ChnkCheckFlags)
	if err != nil {
		return nil, err
	}

	// try the file writer
	w := newCWriter(cfg.FileName, chkr.lro, chkr.tSize, cfg.MaxChunkSize)
	if cfg.WriteFlushMs > 0 {
		w.flushTO = time.Duration(cfg.WriteFlushMs) * time.Millisecond
	}

	if cfg.WriteIdleSec > 0 {
		w.idleTO = time.Duration(cfg.WriteIdleSec) * time.Second
	}

	c := new(Chunk)
	c.fileName = cfg.FileName
	c.fdPool = fdPool
	c.w = w
	c.logger = log4g.GetLogger("chunk").WithId(lid).(log4g.Logger)
	c.logger.Info("New chunk: ", c, ", checker: ", chkr)
	return c, nil
}

// Write allows to write records to the chunk.
func (c *Chunk) Write(ctx context.Context, it records.Iterator) (int, int64, error) {
	return c.w.write(ctx, it)
}

func (c *Chunk) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed {
		c.logger.Error("An attempt to close already closed chunk")
		return ErrWrongState
	}
	c.logger.Info("Closing the chunk: ")
	c.closed = true

	c.fdPool.releaseAllByName(c.fileName)
	return nil
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{file: %s, writer: %s, closed:%t}", c.fileName, c.w, c.closed)
}
