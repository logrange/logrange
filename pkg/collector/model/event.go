package model

import (
	"encoding/json"
	"github.com/logrange/range/pkg/util"
)

// Event is a structure which contains a list of records, parsed from the sourced file
type Event struct {

	// File contains filename where the records come from
	File string `json:"file"`
	// Records list of parsed record, that come from the file
	Records []*Record `json:"records"`

	// confCh is handling the event notification channel. The consumer,
	// after handling the event must perform a READ operation from the channel,
	// signalling the scanner about successful event handling transaction
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
