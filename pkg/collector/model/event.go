package model

import (
	"encoding/json"
	"github.com/logrange/logrange/pkg/util"
)

// Event is a structure which contains a list of records,
// parsed from a file. Event has a consumption confirmation mechanism,
// consumer must call Confirm() as soon as he finished handling the even.
type Event struct {

	// File contains filename of the file from where the records come
	File string `json:"file"`

	// Records are the list of parsed records
	Records []*Record `json:"records"`

	// confCh is a signaling channel, to notify scanner that even was handled
	confCh chan struct{}
}

func NewEvent(file string, recs []*Record, confCh chan struct{}) *Event {
	return &Event{
		File:    file,
		Records: recs,
		confCh:  confCh,
	}
}

func (e *Event) MarshalJSON() ([]byte, error) {
	type alias Event
	return json.Marshal(&struct {
		*alias
		Records int `json:"records"`
	}{
		alias:   (*alias)(e),
		Records: len(e.Records),
	})
}

// consumer must call Confirm() as soon as the event is handled
func (e *Event) Confirm() bool {
	var ok bool
	if e.confCh != nil {
		_, ok = <-e.confCh
		e.confCh = nil
	}
	return ok
}

func (e *Event) String() string {
	return util.ToJsonStr(e)
}
