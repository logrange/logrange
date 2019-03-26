package sink

import (
	"fmt"
	"github.com/logrange/logrange/api"
)

type (
	stdoutSinkConfig struct {
	}
	stdoutSink struct {
	}
)

//===================== stdoutSink =====================

func newStdSkink(cfg *stdoutSinkConfig) (*stdoutSink, error) {
	return &stdoutSink{}, nil
}

func (ss *stdoutSink) OnEvent(events []*api.LogEvent) error {
	for _, e := range events {
		fmt.Print(e.Message)
	}
	return nil
}

func (ss *stdoutSink) Close() error {
	return nil
}
