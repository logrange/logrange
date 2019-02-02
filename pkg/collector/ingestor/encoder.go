package ingestor

import (
	"github.com/logrange/logrange/pkg/collector/model"
	"github.com/logrange/logrange/pkg/logevent"
	"github.com/logrange/range/pkg/records"
)

// encoder structs intends to form a binary package will be send by Atmosphere
type encoder struct {
	bwriter records.Writer
	buf     []byte
}

// newEncoder creates a new encoder object
func newEncoder() *encoder {
	e := new(encoder)
	e.buf = make([]byte, 4096)
	e.bwriter.Reset(e.buf, true)
	return e
}

// encode forms a binary package for sending it through a wire. It expects header
// and a set of records in ev. As a result it returns slice of bytes or an error
// if any
func (e *encoder) encode(hdr *hdrsCacheRec, ev *model.Event) ([]byte, error) {
	e.bwriter.Reset(e.buf, true)
	bf, err := e.bwriter.Allocate(len(hdr.srcId), true)
	if err != nil {
		return nil, err
	}

	_, err = logevent.MarshalStringBuf(hdr.srcId, bf)
	if err != nil {
		return nil, err
	}

	first := true
	var le logevent.LogEvent
	for _, r := range ev.Records {
		if first {
			le.InitWithTagLine(int64(r.GetTs().UnixNano()), logevent.WeakString(r.Data), hdr.tags)
		} else {
			le.Init(int64(r.GetTs().UnixNano()), logevent.WeakString(r.Data))
		}
		first = false

		rb, err := e.bwriter.Allocate(le.BufSize(), true)
		if err != nil {
			return nil, err
		}
		_, err = le.Marshal(rb)
		if err != nil {
			return nil, err
		}
	}

	e.buf, err = e.bwriter.Close()
	return e.buf, err
}
