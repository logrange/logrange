package fs

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/jrivets/log4g"
)

type (
	// cChecker is a helper structure which allows to check the chunk data consistency
	// and which is used for finding last record offset in the chunk file
	cChecker struct {
		logger   log4g.Logger
		fdPool   *FdPool
		fileName string

		// Last Record offset, could be filled by the checkFileConsistency
		lro int64
		// initial size. Size before the truncation happens (if it was)
		iSize int64
		// truncated size. Actual size after the checks.
		tSize int64
	}
)

// checkFileConsistency checks dat file health or create new one if the file does
// not exist. If the chunk exists and the data is corrupted, the method could
// try to find the unbroken chain of records and truncate the corrupted part.
//
// flags could be:
// ChnkChckTruncateOk - indicates that file data can be truncated to the good state
// ChnkChckFullScan - means that full data scan must be performed instead of quick
// 					  check at the beginning.
func (cc *cChecker) checkFileConsistency(ctx context.Context, flags int) error {
	cc.logger.Info("checkFileConsistency()")
	f, err := os.OpenFile(cc.fileName, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		cc.logger.Warn("checkFileConsistency: Could not open or create file ", cc.fileName, " err=", err)
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		cc.logger.Warn("checkFileConsistency: Could not get file info err=", err)
		return err
	}
	sz := fi.Size()

	cc.lro = -1
	cc.iSize = sz
	cc.tSize = sz
	if sz == 0 {
		cc.logger.Info("checkFileConsistency: Dat file is empty, go ahead with it")
		return nil
	}

	fr, err := cc.fdPool.acquire(ctx, cc.fileName, 0)
	if err != nil {
		cc.logger.Error("checkFileConsistency: Could not obtain fReader, err=", err)
	}
	defer cc.fdPool.release(fr)

	if flags&ChnkChckFullScan != 0 {
		cc.lro, cc.tSize = cc.iterateUntilGoodLastRec(fr, sz)
	} else {
		err = cc.fillLastRec(fr, sz)
	}
	if (err != nil || cc.tSize != cc.iSize) && flags&ChnkChckTruncateOk != 0 {
		cc.logger.Warn("checkFileConsistency:  The chunk seems to be corrupted, will try to truncate the chunk data...")
		if err != nil {
			cc.lro, cc.tSize = cc.iterateUntilGoodLastRec(fr, sz)
		}
		err = os.Truncate(cc.fileName, cc.tSize)
	}
	return err
}

// fillLastRec jumps to the end of chunk and checks the status of sizes for the last
// record. The method doesn't give 100% guarantee of the file consisteny result, but
// at least it checks sizes of the last record.
func (cc *cChecker) fillLastRec(fr *fReader, size int64) error {
	cc.logger.Info("fillLastRec: reading last record, file size=", size)
	err := fr.seek(size - ChnkDataHeaderSize)
	if err != nil {
		cc.logger.Warn("fillLastRec: cannot move to offset=", size, ", err=", err)
		return err
	}

	var szBuf [ChnkDataHeaderSize]byte
	_, err = fr.read(szBuf[:])
	if err != nil {
		cc.logger.Warn("fillLastRec: cannot read bottom size of the last record, err=", err)
		return err
	}
	sz := int(binary.BigEndian.Uint32(szBuf[:]))

	offs := size - int64(sz+2*ChnkDataHeaderSize)
	if sz < 0 || offs < 0 || offs > size {
		cc.logger.Warn("fillLastRec: unexpected last record size=", sz, ", offset for last record is ", offs)
		return ErrCorruptedData
	}

	err = fr.seek(offs)
	if err != nil {
		cc.logger.Warn("fillLastRec: cannot move to the last record, offs=", offs, ", size=", size, ", err=", err)
		return err
	}

	_, err = fr.read(szBuf[:])
	if err != nil {
		cc.logger.Warn("fillLastRec: cannot read top size of the last record, err=", err)
		return err
	}

	nsz := int(binary.BigEndian.Uint32(szBuf[:]))
	if sz != nsz {
		cc.logger.Warn("fillLastRec: unexpected record size on top=", nsz, ", bottom one was ", sz)
		return ErrCorruptedData
	}

	cc.lro = offs
	return nil
}

// iterateUntilGoodLastRec() walks through the file and finds the last good record
// offset (1st parameter). The second param contains the size to which the file
// should be truncated if it contains wrong information.
func (cc *cChecker) iterateUntilGoodLastRec(fr *fReader, size int64) (int64, int64) {
	cc.logger.Info("iterateUntilGoodLastRec: Looking for good last record. File size=", size)
	lro := int64(0)
	sz := int64(0)
	var szBuf [ChnkDataHeaderSize]byte
	rdSlice := szBuf[:]
	rec := int64(1)
	for sz < size {
		err := fr.seek(sz)
		if err != nil {
			cc.logger.Warn("iterateUntilGoodLastRec: cannot move to offset=", sz, ", err=", err, ". Stop at record #", rec)
			return lro, sz
		}

		_, err = fr.read(rdSlice)
		if err != nil {
			cc.logger.Warn("iterateUntilGoodLastRec: cannot read top size of the record #", rec, ", err=", err)
			return lro, sz
		}
		tsz := int64(binary.BigEndian.Uint32(rdSlice))

		offs := tsz + sz + int64(ChnkDataHeaderSize)
		if offs < 0 || offs < sz || tsz < 0 || (offs+ChnkDataHeaderSize) > size {
			cc.logger.Warn("iterateUntilGoodLastRec: wrong bottom record size position, offset=",
				offs, ", size=", size, ", record #", rec)
			return lro, sz
		}

		err = fr.seek(offs)
		if err != nil {
			cc.logger.Warn("iterateUntilGoodLastRec: cannot move to bottom record size. offset=",
				offs, ", err=", err, ". record #", rec)
			return lro, sz
		}

		_, err = fr.read(rdSlice)
		if err != nil {
			cc.logger.Warn("iterateUntilGoodLastRec: cannot read bottom size of the record #",
				rec, ", err=", err)
			return lro, sz
		}
		bsz := int64(binary.BigEndian.Uint32(rdSlice))

		if bsz != tsz {
			cc.logger.Warn("iterateUntilGoodLastRec: bottom size bsz=", bsz, " is not equal to top size=", tsz, " for record #", rec)
			return lro, sz
		}
		rec++
		lro = sz
		sz = offs + ChnkDataHeaderSize
	}
	return lro, sz
}

func (cc *cChecker) String() string {
	return fmt.Sprintf("{fileName=%s, lro=%d, iSize=%d, tSize=%d}", cc.fileName, cc.lro, cc.iSize, cc.tSize)
}
