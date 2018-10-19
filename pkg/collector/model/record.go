package model

import (
	"time"
)

type Record struct {
	Data []byte                 `json:"data"`
	Meta map[string]interface{} `json:"meta"`
}

const TsField = "timestamp"

func NewEmptyRecord() *Record {
	return &Record{
		Meta: map[string]interface{}{},
	}
}

func (r *Record) GetData() []byte {
	return r.Data
}

func (r *Record) SetData(data []byte) {
	r.Data = data
}

func (r *Record) GetTs() time.Time {
	if t := r.GetMeta(TsField); t != nil {
		return t.(time.Time)
	}
	return time.Time{}
}

func (r *Record) SetTs(t time.Time) {
	r.SetMeta(TsField, t)
}

func (r *Record) GetMeta(k string) interface{} {
	v, _ := r.Meta[k]
	return v
}

func (r *Record) SetMeta(k string, v interface{}) {
	r.Meta[k] = v
}
