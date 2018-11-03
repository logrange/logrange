package chunkfs

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/jrivets/log4g"
)

type (
	IdxChecker struct {
		dr     *fReader
		ir     *fReader
		logger log4g.Logger
	}
)

// LightCheck checks that idx file is consistent with data file. It reads
// last record from the index file and checks whether it is consistent with
// last record of data file. Returns an error if the data inconsistency is found
// or some file operation is failed
func (ic *IdxChecker) LightCheck() error {
	ic.logger.Debug("LightCheck(): Running")

	ic.ir.seekToEnd()
	pos := ic.ir.getNextReadPos()
	ic.dr.seekToEnd()
	dsize := ic.dr.getNextReadPos()
	if pos == 0 {
		if dsize == 0 {
			ic.logger.Info("LightCheck(): Ok. both files are empty")
			return nil
		}
		return fmt.Errorf("LightCheck(): Incorrect Index size. Index size is 0, but data file size is %d", dsize)
	}

	if pos%ChnkIndexRecSize != 0 {
		return fmt.Errorf("LightCheck(): Inocrrect Index size. The index file size %d must be divided withour reminder on %d", pos, ChnkIndexRecSize)
	}

	ic.ir.seek(pos - ChnkIndexRecSize)

	var offsArr [ChnkIndexRecSize]byte
	offsBuf := offsArr[:]
	_, err := ic.ir.read(offsBuf)
	if err != nil {
		return err
	}

	offs := int64(binary.BigEndian.Uint64(offsBuf))
	if offs < 0 || offs > dsize-4 {
		return fmt.Errorf("LightCheck(): Wrong offset for the last record=%d, which must be in the range [0..%d]", offs, dsize-4)
	}

	ic.dr.seek(offs)

	// the last record size
	_, err = ic.dr.read(offsBuf[:4])
	if err != nil {
		return err
	}
	sz := int(binary.BigEndian.Uint32(offsBuf))
	if offs+int64(sz+4) != dsize {
		return fmt.Errorf("LightCheck(): Wrong last record size. offset=%d. Found record size=%d, but data file size=%d", offs, sz, dsize)
	}

	ic.logger.Info("LightCheck(): Ok.")
	return nil
}

// FullCheck scans all records from the data file to check whether they are correctly indexed
// by the index file. Returns an error if the data inconsistency is found
// or some file operation is failed
func (ic *IdxChecker) FullCheck() error {
	ic.logger.Debug("FullCheck(): Scanning")

	ic.ir.seek(0)
	ic.dr.seekToEnd()
	dsize := ic.dr.getNextReadPos()
	ic.dr.seek(0)

	cnt := 0
	var offsArr [ChnkIndexRecSize]byte
	offsBuf := offsArr[:]
	for {
		_, err := ic.ir.read(offsBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		offs := int64(binary.BigEndian.Uint64(offsBuf))
		if offs < 0 || offs != ic.dr.getNextReadPos() {
			return fmt.Errorf("FullCheck(): Wrong offset for the record %d, its offset=%d, but expected %d", cnt, offs, ic.dr.getNextReadPos())
		}

		_, err = ic.dr.read(offsBuf[:4])
		if err != nil {
			return err
		}
		sz := int(binary.BigEndian.Uint32(offsBuf))

		offs = int64(sz) + ic.dr.getNextReadPos()
		if offs > dsize {
			return fmt.Errorf("FullCheck(): Wrong size=%d for record %d. It is out of the chunk data file size=%d", sz, cnt, dsize)
		}

		ic.dr.seek(int64(sz) + ic.dr.getNextReadPos())
		cnt++
	}

	if ic.dr.getNextReadPos() != dsize {
		return fmt.Errorf("FullCheck(): corrupted index or data file. Index is over, but data file still has some data.")
	}

	ic.logger.Info("FullCheck(): Ok. ", cnt, " records found, index and data looks good.")
	return nil
}

// Recover rebuilds index file by the chunk data file, It scans the data file and writes
// the new index file. Will truncate the data file if data corruption is found,
// but only if fixData=true. Returns the error if the operation could not be
// completed
func (ic *IdxChecker) Recover(fixData bool) error {
	idxfn := ic.ir.filename
	os.Remove(idxfn)
	iw, err := newFWriter(idxfn, ChnkWriterBufSize)
	if err != nil {
		ic.ir.Close()
		ic.logger.Error("Recover(): could not create index file. err=", err, ", operation aborted.")
		return err
	}

	ic.dr.seekToEnd()
	dsize := ic.dr.getNextReadPos()
	ic.dr.seek(0)

	cnt := 0
	var offsArr [ChnkIndexRecSize]byte
	offsBuf := offsArr[:]
	tPos := int64(0)
	for {
		gdPos := ic.dr.getNextReadPos()
		_, err = ic.dr.read(offsBuf[:4])
		if err != nil {
			break
		}
		sz := int(binary.BigEndian.Uint32(offsBuf))

		nPos := int64(sz) + 4 + gdPos
		if nPos > dsize {
			ic.logger.Warn("Recover(): found wrong size=", sz, " at the record ", cnt, ", which greate than data file size=", dsize, ", gdPos=", gdPos)
			err = ErrCorruptedData
			break
		}
		ic.dr.seek(nPos)
		tPos = nPos

		binary.BigEndian.PutUint64(offsBuf, uint64(gdPos))
		_, err = iw.write(offsBuf)
		if err != nil {
			ic.logger.Error("Recover(): could not write offset=", gdPos, " for record ", cnt, " to the index file. err=", err)
			break
		}
		cnt++
	}

	switch err {
	case io.EOF:
		err = nil
	case ErrCorruptedData:
		ic.logger.Warn("Recover(): data file is corrupted. Good size is ", tPos, ", but total file size is ", dsize)
		if fixData {
			ic.logger.Warn("Recover(): Truncating data file size to ", tPos)
			err = os.Truncate(ic.dr.filename, tPos)
		}
	}

	err1 := iw.Close()
	err2 := ic.ir.reopen()
	if err == nil {
		err = err1
		if err == nil {
			err = err2
		}
	}

	ic.logger.Info("Recover(): completed. Initial file size=", dsize, ", truncated at ", tPos, " err=", err)
	return err
}
