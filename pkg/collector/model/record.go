package model

import (
	"time"
)

// Record is a file entry (e.g. log line) represented as a slice of bytes
// plus some parsed out (or additional) meta information attached to it.
// Sometimes it's convenient to parse out the needed info from the file entry
// on the client side and not backend, it saves backend resources. Also
// backend may simply not know some information which client knows (e.g.
// containerId).
type Record struct {
	Data []byte                 `json:"data"`
	Meta map[string]interface{} `json:"meta"`
}

const dateField = "datetime"

func NewRecord(data []byte, ts time.Time) *Record {
	r := &Record{
		Data: data,
		Meta: map[string]interface{}{},
	}

	r.setDate(ts)
	return r
}

func (r *Record) GetData() []byte {
	return r.Data
}

func (r *Record) setData(data []byte) {
	r.Data = data
}

func (r *Record) GetDate() time.Time {
	if t := r.GetMeta(dateField); t != nil {
		return t.(time.Time)
	}
	return time.Time{}
}

func (r *Record) setDate(t time.Time) {
	r.SetMeta(dateField, t)
}

func (r *Record) GetMeta(k string) interface{} {
	v, _ := r.Meta[k]
	return v
}

func (r *Record) SetMeta(k string, v interface{}) {
	r.Meta[k] = v
}
