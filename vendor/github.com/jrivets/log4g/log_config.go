package log4g

import (
	"errors"
	"strconv"
	"strings"

	"github.com/jrivets/gorivets"
)

type logConfig struct {
	loggers          map[string]*logger
	logLevels        *gorivets.SortedSlice
	logContexts      *gorivets.SortedSlice
	appenderFactorys map[string]AppenderFactory
	appenders        map[string]Appender
	levelNames       []string
	levelMap         map[string]Level
}

// Config params
const (
	// appender.console.type=log4g/consoleAppender
	cfgAppender     = "appender"
	cfgAppenderType = "type"

	//level.11=SEVERE
	cfgLevel = "level"

	// context.a.b.c.appenders=console,ROOT
	// context.a.b.c.level=INFO
	// context.a.b.c.buffer=100
	cfgContext          = "context"
	cfgContextAppenders = "appenders"
	cfgContextLevel     = "level"
	cfgContextBufSize   = "buffer"
	cfgContextBlocking  = "blocking"
	cfgContextInherited = "inherited"

	// logger.a.b.c.d.level=INFO
	cfgLogger      = "logger"
	cfgLoggerLevel = "level"
)

const rootLoggerName = ""

var defaultConfigParams = map[string]string{
	"appender.ROOT.layout": "[%d{01-02 15:04:05.000}] %p %c%i: %m",
	"appender.ROOT.type":   consoleAppenderName,
	"context.appenders":    "ROOT",
	"context.level":        "INFO"}

func newLogConfig() *logConfig {
	lc := &logConfig{}

	lc.loggers = make(map[string]*logger)
	lc.logLevels, _ = gorivets.NewSortedSlice(10)
	lc.logContexts, _ = gorivets.NewSortedSlice(2)
	lc.appenderFactorys = make(map[string]AppenderFactory)
	lc.appenders = make(map[string]Appender)
	lc.levelNames = make([]string, ALL+1)
	lc.levelMap = make(map[string]Level)

	lc.levelNames[FATAL] = "FATAL"
	lc.levelNames[ERROR] = "ERROR"
	lc.levelNames[WARN] = "WARN "
	lc.levelNames[INFO] = "INFO "
	lc.levelNames[DEBUG] = "DEBUG"
	lc.levelNames[TRACE] = "TRACE"
	lc.levelNames[ALL] = "ALL  "

	return lc
}

func mergedParamsWithDefault(params map[string]string) map[string]string {
	result := map[string]string{}
	for k, v := range params {
		result[k] = v
	}
	for k, v := range defaultConfigParams {
		_, ok := result[k]
		if !ok {
			result[k] = v
		}
	}
	return result
}

// Check whether ROOT context exists, if no, it will be initialized by default
func (lc *logConfig) initIfNeeded() {
	if getLogLevelContext(rootLoggerName, lc.logContexts) != nil {
		return
	}
	lc.setConfigParams(defaultConfigParams)
}

func (lc *logConfig) cleanUp() {
	defer gorivets.EndQuietly()

	for _, ctx := range lc.logContexts.Copy() {
		ctx.(*logContext).shutdown()
	}

	for _, app := range lc.appenders {
		app.Shutdown()
	}
}

func (lc *logConfig) initWithParams(oldLogConfig *logConfig, params map[string]string) {
	for k, v := range oldLogConfig.loggers {
		lc.loggers[k] = v
	}

	for k, v := range oldLogConfig.appenderFactorys {
		lc.appenderFactorys[k] = v
	}

	lc.levelNames = oldLogConfig.levelNames
	lc.logLevels, _ = gorivets.NewSortedSliceByParams(oldLogConfig.logLevels.Copy()...)
	lc.setConfigParams(params)
}

func (lc *logConfig) setConfigParams(params map[string]string) {
	lc.applyLevelParams(params)
	lc.createAppenders(params)
	lc.createContexts(params)
	lc.createLoggers(params)
	lc.applyLevelsAndContexts()
}

// Allows to specify custom level names in form level.X=<levelName>
// for example: level.11=SEVERE
func (lc *logConfig) applyLevelParams(params map[string]string) {
	levels := groupConfigParams(params, cfgLevel, nil)
	lvlMap, ok := levels[""]
	if ok {
		for levelNum, levelName := range lvlMap {
			level, err := strconv.Atoi(levelNum)
			if err != nil || level < 0 || level > int(ALL) {
				panic("Incorrect log level id=" + strconv.Itoa(level) +
					" provided with \"" + levelName + "\": the id should be in [0.." + strconv.Itoa(int(ALL)) + "]")
			}
			// need to remove previous name from lc.levelMap to avoid 2+ names refer to the same level number
			delete(lc.levelMap, strings.Trim(strings.ToLower(lc.levelNames[level]), " "))
			lc.levelNames[level] = levelName
		}
	}

	for i, _ := range lc.levelNames {
		levelName := strings.Trim(strings.ToLower(lc.levelNames[i]), " ")
		if len(levelName) > 0 {
			lc.levelMap[levelName] = Level(i)
		}
	}
}

func (lc *logConfig) createAppenders(params map[string]string) {
	// collect settings for all appenders from config
	apps := groupConfigParams(params, cfgAppender, isCorrectAppenderName)

	// create appenders
	for appName, appAttributes := range apps {
		t := appAttributes[cfgAppenderType]
		f, ok := lc.appenderFactorys[t]
		if !ok {
			panic("Cannot construct appender \"" + t + "\" no appender factory is registered for the appender name.")
		}

		app, err := f.NewAppender(appAttributes)
		if err != nil {
			panic(err.Error())
		}

		lc.appenders[appName] = app
	}
}

func (lc *logConfig) createContexts(params map[string]string) {
	// collect settings for all contexts from config
	ctxs := groupConfigParams(params, cfgContext, isCorrectLoggerName)

	// create contexts
	for logName, ctxAttributes := range ctxs {
		appenders := lc.getAppendersFromList(ctxAttributes[cfgContextAppenders])
		if len(appenders) == 0 {
			panic("Context \"" + logName + "\" doesn't refer to at least one declared appender.")
		}

		levelName := ctxAttributes[cfgContextLevel]
		level := INFO
		if len(levelName) > 0 {
			level = lc.getLevelByName(levelName)
			if level < 0 {
				panic("Unknown log level \"" + levelName + "\" for context \"" + logName + "\"")
			}
		}

		bufSize, err := gorivets.ParseInt64(ctxAttributes[cfgContextBufSize], 0, 100000, 0)
		if err != nil {
			panic("Incorrect buffer size=" + ctxAttributes[cfgContextBufSize] +
				" value for context \"" + logName + "\" should be positive integer: " + err.Error())
		}

		inh, err := gorivets.ParseBool(ctxAttributes[cfgContextInherited], true)
		if err != nil {
			panic("Incorrect context attibute " + cfgContextInherited + " value, should be true or false")
		}

		blocking, err := gorivets.ParseBool(ctxAttributes[cfgContextBlocking], true)
		if err != nil {
			panic("Incorrect context attibute " + cfgContextBlocking + " value, should be true or false")
		}

		setLogLevel(level, logName, lc.logLevels)
		context, _ := newLogContext(logName, appenders, inh, blocking, int(bufSize))
		lc.logContexts.Add(context)
	}
}

func (lc *logConfig) createLoggers(params map[string]string) {
	// collect settings for all loggers from config
	loggers := groupConfigParams(params, cfgLogger, isCorrectLoggerName)

	// apply logger settings
	for loggerName, loggerAttributes := range loggers {
		levelName := loggerAttributes[cfgLoggerLevel]
		level := INFO
		if len(levelName) > 0 {
			level = lc.getLevelByName(levelName)
			if level < 0 {
				panic("Unknown log level \"" + levelName + "\" for logger \"" + loggerName + "\"")
			}
		}
		setLogLevel(level, loggerName, lc.logLevels)
		lc.getLogger(loggerName)
	}
}

func (lc *logConfig) applyLevelsAndContexts() {
	for _, l := range lc.loggers {
		rootLLS := getLogLevelSetting(l.loggerName, lc.logLevels)
		l.setLogLevelSetting(rootLLS)

		lctx := getLogLevelContext(l.loggerName, lc.logContexts)
		l.setLogContext(lctx)
	}
}

func (lc *logConfig) getLogger(loggerName string) *logger {
	loggerName = normalizeLogName(loggerName)
	l, ok := lc.loggers[loggerName]
	if !ok {
		// Create new logger for the name
		rootLLS := getLogLevelSetting(loggerName, lc.logLevels)
		rootCtx := getLogLevelContext(loggerName, lc.logContexts)
		l = &logger{loggerName, rootLLS, rootCtx, rootLLS.level}
		lc.loggers[loggerName] = l
	}
	return l
}

func (lc *logConfig) setLogLevel(level Level, loggerName string) {
	lls := setLogLevel(level, loggerName, lc.logLevels)
	applyNewLevelToLoggers(lls, lc.loggers)
}

func (lc *logConfig) registerAppender(appenderFactory AppenderFactory) error {
	appenderName := appenderFactory.Name()
	_, ok := lc.appenderFactorys[appenderName]
	if ok {
		return errors.New("Cannot register appender factory for the name " + appenderName +
			" because the name is already registerd ")
	}

	lc.appenderFactorys[appenderName] = appenderFactory
	return nil
}

func (lc *logConfig) getAppendersFromList(appNames string) []Appender {
	appNames = strings.Trim(appNames, " ")
	if len(appNames) == 0 {
		return nil
	}

	names := strings.Split(appNames, ",")
	result := make([]Appender, 0, len(names))
	for _, name := range names {
		name = strings.Trim(name, " ")
		a, ok := lc.appenders[name]
		if !ok {
			panic("Undefined appender name \"" + name + "\"")
		}
		result = append(result, a)
	}
	return result
}

// gets level index, or -1 if not found
func (lc *logConfig) getLevelByName(levelName string) (idx Level) {
	levelName = strings.Trim(strings.ToLower(levelName), " ")
	idx, ok := lc.levelMap[levelName]
	if !ok {
		idx = -1
	}
	return idx
}
