package log4g

import (
	"fmt"
	"time"
)

// This is an internal data the log4g configuration will be applied to
type logger struct {
	loggerName string
	lls        *logLevelSetting
	lctx       *logContext
	logLevel   Level
}

// This is an object, which implements Logger interface which contains some
// dynamic fields like loggerId .
type log_wrapper struct {
	logger   *logger
	loggerId interface{}
}

func wrap(l *logger) *log_wrapper {
	return &log_wrapper{logger: l}
}

func (l *log_wrapper) clone() *log_wrapper {
	return &log_wrapper{logger: l.logger, loggerId: l.loggerId}
}

func (l *log_wrapper) Fatal(args ...interface{}) {
	l.Log(FATAL, args...)
}

func (l *log_wrapper) Error(args ...interface{}) {
	l.Log(ERROR, args...)
}

func (l *log_wrapper) Warn(args ...interface{}) {
	l.Log(WARN, args...)
}

func (l *log_wrapper) Info(args ...interface{}) {
	l.Log(INFO, args...)
}

func (l *log_wrapper) Debug(args ...interface{}) {
	l.Log(DEBUG, args...)
}

func (l *log_wrapper) Trace(args ...interface{}) {
	l.Log(TRACE, args...)
}

func (l *log_wrapper) Log(level Level, args ...interface{}) {
	if l.logger.logLevel < level {
		return
	}
	l.logInternal(level, fmt.Sprint(args...))
}

func (l *log_wrapper) Logf(level Level, fstr string, args ...interface{}) {
	if l.logger.logLevel < level {
		return
	}
	msg := fstr
	if len(args) > 0 {
		msg = fmt.Sprintf(fstr, args...)
	}
	l.logInternal(level, msg)
}

func (l *log_wrapper) Logp(level Level, payload interface{}) {
	if l.logger.logLevel < level {
		return
	}
	l.logInternal(level, payload)
}

func (l *log_wrapper) GetName() string {
	return l.logger.loggerName
}

func (l *log_wrapper) WithId(id interface{}) interface{} {
	if id != l.loggerId {
		lg := l.clone()
		lg.loggerId = id
		return lg
	}
	return l
}

func (l *log_wrapper) WithName(name string) interface{} {
	if l.logger.loggerName != name {
		lg := GetLogger(name)
		return lg.WithId(l.loggerId)
	}
	return l
}

func (l *log_wrapper) GetLevel() Level {
	return l.logger.logLevel
}

func (l *log_wrapper) logInternal(level Level, payload interface{}) {
	l.logger.lctx.log(&Event{level, time.Now(), l.logger.loggerName, l.loggerId, payload})
}

func (l *logger) setLogLevelSetting(lls *logLevelSetting) {
	l.lls = lls
	l.logLevel = lls.level
}

func (l *logger) setLogContext(lctx *logContext) {
	l.lctx = lctx
}

// Apply new LogLevelSetting to all appropriate loggers
func applyNewLevelToLoggers(lls *logLevelSetting, loggers map[string]*logger) {
	for _, l := range loggers {
		if !ancestor(lls.loggerName, l.loggerName) {
			continue
		}
		if ancestor(l.lls.loggerName, lls.loggerName) {
			l.setLogLevelSetting(lls)
		}
	}
}
