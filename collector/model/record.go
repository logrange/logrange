package model

import (
	"time"
)

// Record is a file entry (e.g. log line) represented as a slice of bytes
// plus some parsed out (or additional) meta information attached to it.
// Sometimes it's convenient to parse out the needed info from the file entry
// on the client side and not remote, it saves remote resources. Also
// remote may simply not know some information which client knows (e.g.
// containerId).
type Record struct {

	// Raw bytes, representing file entry read
	Data []byte `json:"data"`

	// Record date
	Date time.Time `json:"date"`

	// Tags, attached to the entry
	Tags map[string]string `json:"meta"`
}

func NewRecord(data []byte, date time.Time) *Record {
	r := &Record{
		Data: data,
		Date: date,
		Tags: make(map[string]string),
	}
	return r
}

func (r *Record) GetData() []byte {
	return r.Data
}

func (r *Record) setData(data []byte) {
	r.Data = data
}

func (r *Record) GetDate() time.Time {
	return r.Date
}

func (r *Record) setDate(t time.Time) {
	r.Date = t
}

func (r *Record) GetTag(k string) string {
	v, _ := r.Tags[k]
	return v
}

func (r *Record) SetTag(k string, v string) {
	r.Tags[k] = v
}
