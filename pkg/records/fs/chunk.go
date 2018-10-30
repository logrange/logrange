package fs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"
)

var (
	ErrWrongOffset    = fmt.Errorf("Error wrong offset while seeking")
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
		MaxRecordSize  int64
		ChkListener    records.ChunkListener
		Id             uint64
	}

	// Chunk struct allows to control read and write operations over the underlying
	// data file
	Chunk struct {
		id       uint64
		fileName string

		lock   sync.Mutex
		fdPool *FdPool
		closed bool
		logger log4g.Logger

		// writing part
		w *cWrtier
		// the chunk listener
		lstnr records.ChunkListener

		// Max Record Size
		maxRecSize int64
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
	ChnkMaxRecordSize = ChnkWriterBufSize

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
	c.id = cfg.Id
	c.maxRecSize = cfg.MaxRecordSize
	if c.maxRecSize <= 0 {
		c.maxRecSize = ChnkMaxRecordSize
	}
	c.logger = log4g.GetLogger("chunk").WithId(lid).(log4g.Logger)
	c.logger.Info("New chunk: ", c, ", checker: ", chkr)
	return c, nil
}

// Id returns the chunk Id
func (c *Chunk) Id() uint64 {
	return c.id
}

// Iterator returns the chunk iterator which could be used for reading the chunk data
func (c *Chunk) Iterator() (records.ChunkIterator, error) {
	buf := make([]byte, c.maxRecSize)
	ci := newCIterator(c.fileName, c.fdPool, &c.w.lroCfrmd, buf)
	return ci, nil
}

// Write allows to write records to the chunk.
func (c *Chunk) Write(ctx context.Context, it records.Iterator) (int, int64, error) {
	return c.w.write(ctx, it)
}

// Size returns the size (unconfirmed) of the chunk
func (c *Chunk) Size() int64 {
	return atomic.LoadInt64(&c.w.size)
}

// GetLastRecordOffset returns synced last record offset (read guarantee)
func (c *Chunk) GetLastRecordOffset() int64 {
	return atomic.LoadInt64(&c.w.lroCfrmd)
}

func (c *Chunk) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed {
		c.logger.Error("An attempt to close already closed chunk")
		return util.ErrWrongState
	}
	c.logger.Info("Closing the chunk: ")
	c.closed = true

	c.fdPool.releaseAllByName(c.fileName)
	return nil
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{file=%s, writer=%s, closed=%t, maxRecSize=%d}", c.fileName, c.w, c.closed, c.maxRecSize)
}

func (c *Chunk) onWriterFlush() {
	if c.lstnr != nil {
		c.lstnr.OnLROChange(c)
	}
}
