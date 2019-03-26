package sink

import (
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/field"
	"github.com/logrange/logrange/pkg/syslog"
	"github.com/mitchellh/mapstructure"
	"time"
)

type (

	//literal or {time}, {message}, {tags}, {vars:key}
	syslogMessageSchemaCfg struct {
		Facility string
		Severity string
		Hostname string
		Tags     string
		Msg      string
	}

	syslogMessageSchema struct {
		facility *model.FormatParser
		severity *model.FormatParser
		hostname *model.FormatParser
		tags     *model.FormatParser
		msg      *model.FormatParser
	}

	syslogConfig struct {
		syslog.Config `mapstructure:",squash"`
		MessageSchema *syslogMessageSchemaCfg
	}

	syslogSink struct {
		cfg  *syslogConfig
		slog *syslog.Logger
		schm *syslogMessageSchema
	}
)

//===================== syslogSink =====================

func newSyslogSink(params Params) (*syslogSink, error) {
	var scfg = &syslogConfig{}

	err := mapstructure.Decode(params, scfg)
	if err != nil {
		return nil, fmt.Errorf("unable to decode Params=%v; %v", params, err)
	}

	ms, err := newSyslogMessageSchema(scfg.MessageSchema)
	if err != nil {
		return nil, fmt.Errorf("unable to parse MessageSchema=%v: %v", scfg.MessageSchema, err)
	}

	slog, err := syslog.NewLogger(&scfg.Config)
	if err != nil {
		return nil, fmt.Errorf("unable to create syslog Logger, err=%v", err)
	}

	return &syslogSink{
		cfg:  scfg,
		slog: slog,
		schm: ms,
	}, nil
}

func (ss *syslogSink) OnEvent(events []*api.LogEvent) error {
	me := &model.LogEvent{}
	sm := &syslog.Message{}
	for _, e := range events {
		copyEv(e, me)

		sm.Facility = syslog.FacilityLocal6
		if ss.schm.facility != nil {
			f, err := syslog.Facility(ss.schm.facility.FormatStr(me, e.Tags))
			if err == nil {
				sm.Facility = f
			}
		}
		sm.Severity = syslog.SeverityInfo
		if ss.schm.severity != nil {
			s, err := syslog.Severity(ss.schm.severity.FormatStr(me, e.Tags))
			if err == nil {
				sm.Severity = s
			}
		}

		sm.Time = time.Unix(0, int64(e.Timestamp))
		sm.Hostname = "localhost"
		if ss.schm.hostname != nil {
			sm.Hostname = ss.schm.hostname.FormatStr(me, e.Tags)
		}
		sm.Tag = e.Tags
		if ss.schm.tags != nil {
			sm.Tag = ss.schm.tags.FormatStr(me, e.Tags)
		}
		sm.Msg = e.Message
		if ss.schm.msg != nil {
			sm.Msg = ss.schm.msg.FormatStr(me, e.Tags)
		}

		err := ss.slog.Write(sm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ss *syslogSink) Close() error {
	if ss.slog != nil {
		return ss.slog.Close()
	}
	return nil
}

func copyEv(e *api.LogEvent, me *model.LogEvent) {
	me.Timestamp = e.Timestamp
	me.Msg = []byte(e.Message)
	me.Fields = field.Fields(e.Fields)
}

//===================== syslogMessageSchemaCfg =====================

func newSyslogMessageSchema(cfg *syslogMessageSchemaCfg) (*syslogMessageSchema, error) {
	var (
		err error
		ms  syslogMessageSchema
	)

	if cfg == nil {
		return &syslogMessageSchema{}, nil
	}
	if cfg.Facility != "" {
		if ms.facility, err = model.NewFormatParser(cfg.Facility); err != nil {
			return nil, err
		}
	}
	if cfg.Severity != "" {
		if ms.severity, err = model.NewFormatParser(cfg.Severity); err != nil {
			return nil, err
		}
	}
	if cfg.Hostname != "" {
		if ms.hostname, err = model.NewFormatParser(cfg.Hostname); err != nil {
			return nil, err
		}
	}
	if cfg.Tags != "" {
		if ms.tags, err = model.NewFormatParser(cfg.Tags); err != nil {
			return nil, err
		}
	}
	if cfg.Msg != "" {
		if ms.msg, err = model.NewFormatParser(cfg.Msg); err != nil {
			return nil, err
		}
	}
	return &ms, nil
}
