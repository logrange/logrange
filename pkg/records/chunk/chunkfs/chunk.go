package chunkfs

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/util"
)

type (
	Config struct {
		BaseDir        string
		ChnkCheckFlags int
		WriteIdleSec   int
		WriteFlushMs   int
		MaxChunkSize   int64
		MaxRecordSize  int64
		ChkListener    chunk.Listener
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
		lstnr chunk.Listener

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

	ChnkWriterBufSize    = 4 * 4096
	ChnkReaderBufSize    = 2 * 4096
	ChnkIdxReaderBufSize = 4096
	ChnkMaxRecordSize    = ChnkWriterBufSize

	// ChnkChckTruncateOk means that chunk data can be truncated to good size
	ChnkChckTruncateOk = 1

	// ChnkChckFullScan means that full scan to find last record must be performed
	ChnkChckFullScan = 2

	ChnkIndexRecSize = 8

	ChnkDataExt  = ".dat"
	ChnkIndexExt = ".idx"
)

var (
	ErrWrongOffset    = fmt.Errorf("Error wrong offset while seeking")
	ErrBufferTooSmall = fmt.Errorf("Too small buffer for read")
	ErrCorruptedData  = fmt.Errorf("Chunk data has an inconsistency in the data inside")
)

// NewChunk returns new chunk using the following params:
// ctx - is used to control the creation of the chunk process. If the underlying
// 		chunk file is not empty, a checker, which will use fReader will be involved
//		the checker can take significant time due to lack of read descriptors or
// 		by the process itself
// cfg - contains configuration settings for the chunk
// fdPool - file descriptors pool, which is used for holding fdReader objects.
func New(ctx context.Context, cfg *Config, fdPool *FdPool) (*Chunk, error) {
	// run the checker
	fileName := MakeChunkFileName(cfg.BaseDir, cfg.Id)
	lid := "{" + fileName + "}"
	chkr := &checker{fileName: fileName, fdPool: fdPool, logger: log4g.GetLogger("chunk.checker").WithId(lid).(log4g.Logger), cid: cfg.Id}
	err := fdPool.register(cfg.Id, frParams{fileName, ChnkReaderBufSize})
	if err != nil {
		return nil, err
	}

	err = fdPool.register(cfg.Id+1, frParams{util.SetFileExt(fileName, ChnkIndexExt), ChnkIdxReaderBufSize})
	if err != nil {
		return nil, err
	}

	err = chkr.checkFileConsistency(ctx, cfg.ChnkCheckFlags)
	if err != nil {
		fdPool.releaseAllByGid(cfg.Id)
		return nil, err
	}

	// try the file writer
	w := newCWriter(fileName, chkr.tSize, cfg.MaxChunkSize, 0)
	if cfg.WriteFlushMs > 0 {
		w.flushTO = time.Duration(cfg.WriteFlushMs) * time.Millisecond
	}

	if cfg.WriteIdleSec > 0 {
		w.idleTO = time.Duration(cfg.WriteIdleSec) * time.Second
	}

	c := new(Chunk)
	c.fileName = fileName
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
func (c *Chunk) Iterator() (chunk.Iterator, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed {
		return nil, util.ErrWrongState
	}

	buf := make([]byte, c.maxRecSize)
	ci := newCIterator(c.id, c.fdPool, &c.w.cntCfrmd, buf) //TODO!!!
	return ci, nil
}

// Write allows to write records to the chunk.
func (c *Chunk) Write(ctx context.Context, it records.Iterator) (int, uint32, error) {
	return c.w.write(ctx, it)
}

// Size returns the size (confirmed) of the chunk
func (c *Chunk) Size() int64 {
	return atomic.LoadInt64(&c.w.sizeCfrmd)
}

// Count return the number of records (confirmed) in the chunk
func (c *Chunk) Count() uint32 {
	return atomic.LoadUint32(&c.w.cntCfrmd)
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

	c.fdPool.releaseAllByGid(c.id)
	return nil
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{file=%s, writer=%s, closed=%t, maxRecSize=%d}", c.fileName, c.w, c.closed, c.maxRecSize)
}

func (c *Chunk) onWriterFlush() {
	if c.lstnr != nil {
		c.lstnr.OnNewData(c)
	}
}

func MakeChunkFileName(baseDir string, cid uint64) string {
	return util.SetFileExt(path.Join(baseDir, fmt.Sprintf("%016X", cid)), ChnkDataExt)
}

func SetChunkDataFileExt(fname string) string {
	return util.SetFileExt(fname, ChnkDataExt)
}

func SetChunkIdxFileExt(fname string) string {
	return util.SetFileExt(fname, ChnkIndexExt)
}
