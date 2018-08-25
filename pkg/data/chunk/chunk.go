package chunk

import (
	"fmt"

	"sync"

	"github.com/dspasibenko/kepler/data"
	flock "github.com/theckman/go-flock"
)

var (
	//	ErrMaxSizeReached = errors.New("Maximum size of the object is reached.")
	//	ErrUnableToWrite  = errors.New("The write is not possible")
	ErrWrongOffset = fmt.Errorf("Error wrong offset while seeking")

//	ErrUnableToRead   = errors.New("Chunk is closed or not ready for read")
//	ErrWrongState     = errors.New("Wrong state, probably already closed.")
//	ErrBufferTooSmall = errors.New("Too small buffer for read")
//	ErrCorruptedData  = errors.New("Dat file has inconsistent data inside")
//	ErrRecordTooBig   = errors.New("Record is too big. Unacceptable.")
//	ErrCancelled      = errors.New("Operation is cancelled")

//	MinRecordId = RecordId{}
//	MaxRecordId = RecordId{ChunkId: math.MaxUint32, Offset: math.MaxInt64}

//	glock sync.Mutex
//	// chunk read descriptor closer keeps counting on opened desriptors
//	chnk_rd_clsr *chunkRdCloser
)

type (
	Config struct {
		FileName string
	}

	Chunk struct {
		fileName string
		fLock    *flock.Flock
		lock     sync.Mutex
		state    int
	}
)

const (
	cCS_CHECKING = iota
	cCS_OK
	cCS_ERROR
	cCS_CLOSED
)

func New(cfg *Config) (*Chunk, error) {
	fileLock := flock.NewFlock(cfg.FileName)
	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, fmt.Errorf("Could not obtain lock on file %s. Already locked.", cfg.FileName)
	}

	c := new(Chunk)
	c.fileName = cfg.FileName
	c.fLock = fileLock

}

func (c *Chunk) Write(it data.Iterator) error {
	return nil
}

func (c *Chunk) GetReader() (*ChunkReader, error) {
	return nil, nil
}

func (c *Chunk) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.state != cCS_CHECKING && c.state != cCS_OK {
		return fmt.Errorf("Invalid state for closing expected %d or %d, but state=%d", cCS_CHECKING, cCS_OK, c.state)
	}

	c.state = cCS_CLOSED
	return nil
}
