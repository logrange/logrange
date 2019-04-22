package gorivets

import (
	"fmt"
	"sync"
	"time"
)

type Logger interface {
	Fatal(args ...interface{})
	Error(args ...interface{})
	Warn(args ...interface{})
	Info(args ...interface{})
	Debug(args ...interface{})
	Trace(args ...interface{})

	WithId(id interface{}) interface{}
	WithName(name string) interface{}
}

type NewLoggerF func(name string) Logger

func NewNilLoggerProvider() NewLoggerF {
	return func(name string) Logger {
		return nil_logger{}
	}
}

func NewStubLoggerProvider() NewLoggerF {
	return func(name string) Logger {
		return &log{name: name, enabled: true}
	}
}

type nil_logger struct {
}

type log struct {
	name    string
	enabled bool
	id      interface{}
	mx      sync.Mutex
}

func (log *log) Fatal(args ...interface{}) {
	log.log("FATAL", args...)
}

func (log *log) Error(args ...interface{}) {
	log.log("ERROR", args...)
}

func (log *log) Warn(args ...interface{}) {
	log.log("WARN", args...)
}

func (log *log) Info(args ...interface{}) {
	log.log("INFO", args...)
}

func (log *log) Debug(args ...interface{}) {
	log.log("DEBUG", args...)
}

func (log *log) Trace(args ...interface{}) {
	log.log("TRACE", args...)
}

func (lg *log) WithId(id interface{}) interface{} {
	return &log{name: lg.name, enabled: lg.enabled, id: id}
}

func (lg *log) WithName(name string) interface{} {
	if lg.name == name {
		return lg
	}
	return &log{name: name, enabled: true, id: lg.id}
}

func (log *log) log(level string, args ...interface{}) {
	if !log.enabled {
		return
	}

	log.mx.Lock()
	defer log.mx.Unlock()

	fmt.Print(time.Now().Format("15:04:05.999"), " [", level, "] ", log.name, ": ")
	fmt.Println(args...)
}

func (log nil_logger) Fatal(args ...interface{}) {
}

func (log nil_logger) Error(args ...interface{}) {
}

func (log nil_logger) Warn(args ...interface{}) {
}

func (log nil_logger) Info(args ...interface{}) {
}

func (log nil_logger) Debug(args ...interface{}) {
}

func (log nil_logger) Trace(args ...interface{}) {
}

func (log nil_logger) WithId(id interface{}) interface{} {
	return log
}

func (log nil_logger) WithName(name string) interface{} {
	return log
}
