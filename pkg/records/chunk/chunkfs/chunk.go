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

package chunkfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/util"
)

type (
	// Config struct is used for providing configuration for Recover() and
	// New() functions.
	Config struct {
		// The chunk full file name. The fields must not be empty.
		FileName string

		// The chunk Id. If the value is 0, then the Id will be tried to be
		// generated from FileName. An error will be reported if it's not possible
		Id chunk.Id

		// CheckDisabled defines whether the chunk check procedure could be
		// skipped. It could be useful when a new already checked chunk is
		// created.
		CheckDisabled bool

		// CheckFullScan defines that the FullCheck() of IdxChecker will be
		// called. Normally LightCheck() is called only.
		CheckFullScan bool

		// RecoverDisabled flag defines that actual recover procedure should not
		// be run when the check chunk data test is failed.
		RecoverDisabled bool

		// RecoverLostDataOk flag defines that the chunk file could be truncated
		// if the file is partually corrupted.
		RecoverLostDataOk bool

		// WriteIdleSec specifies the writer idle timeout. It will be closed if no
		// write ops happens during the timeout
		WriteIdleSec int

		// WriteFlushMs specifies data sync timeout. Buffers will be synced after
		// last write operation.
		WriteFlushMs int

		// MaxChunkSize defines the maximum chunk size.
		MaxChunkSize int64

		// MaxRecordSize defines the maimum one record size
		MaxRecordSize int64
	}

	// Chunk struct allows to control read and write operations over the underlying
	// data file
	Chunk struct {
		id       chunk.Id
		gid      uint64
		fileName string

		lock   sync.Mutex
		fdPool *FdPool
		closed bool
		logger log4g.Logger

		// writing part
		w *cWrtier
		// the chunk listener
		lstnr atomic.Value

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

	log = log4g.GetLogger("chunkfs")

	// gId is FdPool group id generator. Used by chunks objects to obtain readers
	// out of the pool
	gId int64
)

// MakeChunkFileName compose full chunk file name having the directory name
// and the chunk Id.
func MakeChunkFileName(baseDir string, cid chunk.Id) string {
	return SetChunkDataFileExt(path.Join(baseDir, cid.String()))
}

// SetChunkDataFileExt sets chunk data file extension for a file
func SetChunkDataFileExt(fname string) string {
	return util.SetFileExt(fname, ChnkDataExt)
}

// SetChunkIdxFileExt sets chunk index file extension for a file
func SetChunkIdxFileExt(fname string) string {
	return util.SetFileExt(fname, ChnkIndexExt)
}

// Recover checks the chunk data file and recovers it if it is possible. The
// recovery means to determine longest chain of records, that have good references
// according to the format. If chain of records is broken before the end of the file, the
// remaining part can be considered corrupted and deleted. The
// RecoverLostDataOk flag from Config structure controls it.
//
// If the chunk data inconsistency found and/or it was recovered, the index
// file will be rebuilt as well. Data inconsistency can determined with the index
// file only (data chunk looks good). This case only the index file will be
// rebuilt.
//
// The method can block calling go-routine for unpredictable time, while the
// recovery procedure is completed, an error happens, or the provided context
// is closed.
//
// The following parameters are expected:
// ctx - a calling context. The function will return a result ASAP if the ctx
// 		is closed.
// cfg - the chunk configuration. The Config struct contains the chunk settings
// 		and the chunk checking and recovering procedures settings.
// fdPool - a pointer to FdPool struct to have
func Recover(ctx context.Context, cfg Config, fdPool *FdPool) error {
	_, err := recoverAndNew(ctx, &cfg, fdPool, false)
	return err
}

// New creates a new chunk or returns an error if it is not possible.
// The method does the same action, what Recovery (please see) does. If the
// chunk recovery is ok or it is not needed the Chunk object will be created.
// The function returns an error, if any.
//
// The following parameters are expected:
// ctx - a calling context. The function will return a result ASAP if the ctx
// 		is closed.
// cfg - the chunk configuration. The Config struct contains the chunk settings
// 		and the chunk checking and recovering procedures settings.
// fdPool - a pointer to FdPool struct to have
func New(ctx context.Context, cfg Config, fdPool *FdPool) (*Chunk, error) {
	return recoverAndNew(ctx, &cfg, fdPool, true)
}

func recoverAndNew(ctx context.Context, cfg *Config, fdPool *FdPool, newChunk bool) (*Chunk, error) {
	err := cfg.updateIdByFileName()
	if err != nil {
		return nil, err
	}

	log := log4g.GetLogger("chunkfs").WithId("{" + cfg.Id.String() + "}").(log4g.Logger)
	log.Debug("recoverAndNew(): cfg=", cfg, ", newChunk=", newChunk)

	var c *Chunk

	// register group Ids
	gid := uint64(atomic.AddInt64(&gId, 2) - 2)
	log.Debug("Registering gid=", gid, " for a new chunk")
	err = fdPool.register(gid, frParams{cfg.FileName, ChnkReaderBufSize})
	if err != nil {
		return nil, err
	}
	err = fdPool.register(gid+1, frParams{SetChunkIdxFileExt(cfg.FileName), ChnkIdxReaderBufSize})
	if err != nil {
		fdPool.releaseAllByGid(gid)
		return nil, err
	}
	defer func() {
		if c == nil {
			log.Debug("Releasing gid=", gid, ", cause no chunk is created")
			fdPool.releaseAllByGid(gid)
			fdPool.releaseAllByGid(gid + 1)
		}
	}()

	// checking the chunk...
	err = checkAndRecover(ctx, cfg, fdPool, gid)
	if err != nil {
		log.Warn("recoverAndNew(): could not recover. err=", err)
		return nil, err
	}

	// new chunk?
	if newChunk {
		sz := int64(0)
		if fi, err := os.Stat(cfg.FileName); err != nil && !os.IsNotExist(err) {
			log.Error("recoverAndNew(): could not get file info for the chunk file=", cfg.FileName, ", err=", err)
			return nil, err
		} else if err == nil {
			sz = fi.Size()
		}

		w := newCWriter(cfg.FileName, sz, cfg.MaxChunkSize, 0)
		if cfg.WriteFlushMs > 0 {
			w.flushTO = time.Duration(cfg.WriteFlushMs) * time.Millisecond
		}

		if cfg.WriteIdleSec > 0 {
			w.idleTO = time.Duration(cfg.WriteIdleSec) * time.Second
		}

		c = new(Chunk)
		c.id = cfg.Id
		c.gid = gid
		c.fileName = cfg.FileName
		c.fdPool = fdPool
		c.w = w
		c.id = cfg.Id
		c.maxRecSize = cfg.MaxRecordSize
		if c.maxRecSize <= 0 {
			c.maxRecSize = ChnkMaxRecordSize
		}
		c.logger = log
		c.AddListener(chunk.EmptyListener)
	}

	return c, err
}

func checkAndRecover(ctx context.Context, cfg *Config, fdPool *FdPool, gid uint64) error {
	if _, err := os.Stat(cfg.FileName); os.IsNotExist(err) {
		log.Debug("No file ", cfg.FileName, " skipping the check then.")
		return nil
	}

	if cfg.CheckDisabled {
		log.Debug("Chunk check is disabled, skipping the check step.")
		return nil
	}

	dr, err := fdPool.acquire(ctx, gid, 0)
	if err != nil {
		log.Debug("Could not acquire reader gid=", gid, " from the pool. err=", err)
		return err
	}
	defer fdPool.release(dr)

	ir, err := fdPool.acquire(ctx, gid+1, 0)
	if err != nil {
		log.Debug("Could not acquire reader gid=", gid+1, " from the pool. err=", err)
		fdPool.release(dr)
		return err
	}
	defer fdPool.release(ir)

	ic := IdxChecker{dr, ir, ctx, log}
	if !cfg.CheckFullScan {
		log.Debug("Running light check")
		err = ic.LightCheck()
	} else {
		log.Debug("Running full check")
		err = ic.FullCheck()
	}

	if err != nil && !cfg.RecoverDisabled {
		log.Debug("Running recovery...")
		err = ic.Recover(cfg.RecoverLostDataOk)
	}

	return err
}

// ----------------------------- Config --------------------------------------
func (cfg *Config) updateIdByFileName() error {
	if len(cfg.FileName) == 0 {
		return fmt.Errorf("FileName could not be empty")
	}

	_, file := path.Split(cfg.FileName)
	fid := util.SetFileExt(file, "")
	cid, err := chunk.ParseId(fid)
	if cfg.Id == 0 {
		if err == nil {
			cfg.Id = cid
			log.Debug("Updating value of chunk Id to ", cid, " based on the config filename=", file)
			return nil
		}
		return fmt.Errorf("Unable to generate chunk Id based of the file=%s, the err=%s", file, err)
	}

	if cfg.Id != cid {
		log.Warn("The parsed value ", cid, " doesn't match with config setting ", cfg.Id, ", parsing err=", err)
	}

	return nil
}

func (cfg *Config) String() string {
	var b strings.Builder
	b.WriteString("{Id: ")
	b.WriteString(cfg.Id.String())
	b.WriteString("CheckDisabled: ")
	b.WriteString(strconv.FormatBool(cfg.CheckDisabled))
	b.WriteString("CheckFullScan: ")
	b.WriteString(strconv.FormatBool(cfg.CheckFullScan))
	b.WriteString("RecoverDisabled: ")
	b.WriteString(strconv.FormatBool(cfg.RecoverDisabled))
	b.WriteString("RecoverLostDataOk: ")
	b.WriteString(strconv.FormatBool(cfg.RecoverLostDataOk))
	b.WriteString("WriteIdleSec: ")
	b.WriteString(strconv.FormatInt(int64(cfg.WriteIdleSec), 10))
	b.WriteString("WriteFlushMs: ")
	b.WriteString(strconv.FormatInt(int64(cfg.WriteFlushMs), 10))
	b.WriteString("MaxChunkSize: ")
	b.WriteString(strconv.FormatInt(cfg.MaxChunkSize, 10))
	b.WriteString("MaxRecordSize: ")
	b.WriteString(strconv.FormatInt(cfg.MaxRecordSize, 10))
	b.WriteString("}")
	return b.String()
}

// ------------------------------ Chunk --------------------------------------
// Id returns the chunk Id
func (c *Chunk) Id() chunk.Id {
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
	ci := newCIterator(c.gid, c.fdPool, &c.w.cntCfrmd, buf) //TODO!!!
	return ci, nil
}

// Write allows to write records to the chunk.
func (c *Chunk) Write(ctx context.Context, it records.Iterator) (int, uint32, error) {
	return c.w.write(ctx, it)
}

// Sync flushes the written data to the fs
func (c *Chunk) Sync() {
	c.w.flush()
}

// Size returns the size (confirmed) of the chunk
func (c *Chunk) Size() int64 {
	return atomic.LoadInt64(&c.w.sizeCfrmd)
}

// Count return the number of records (confirmed) in the chunk
func (c *Chunk) Count() uint32 {
	return atomic.LoadUint32(&c.w.cntCfrmd)
}

// Close closes the chunk and releases all resources it holds
func (c *Chunk) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.closed {
		c.logger.Error("An attempt to close already closed chunk")
		return util.ErrWrongState
	}
	c.logger.Info("Closing the chunk: ")
	c.closed = true

	c.fdPool.releaseAllByGid(c.gid)
	c.fdPool.releaseAllByGid(c.gid + 1)
	return c.w.Close()
}

// AddListener sets the chunk's listener to lstnr
func (c *Chunk) AddListener(lstnr chunk.Listener) {
	if lstnr != nil {
		c.lstnr.Store(lwrapper{lstnr})
	}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("{Id: %s, gid: %d, file=%s, writer=%s, closed=%t, maxRecSize=%d}", c.id, c.gid, c.fileName, c.w, c.closed, c.maxRecSize)
}

func (c *Chunk) onWriterFlush() {
	lstnr := c.lstnr.Load().(chunk.Listener)
	lstnr.OnNewData(c)
}
