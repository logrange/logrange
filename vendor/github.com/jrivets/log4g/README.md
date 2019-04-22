# Log4g - Go (golang) Logging Library

[![Build Status](https://travis-ci.org/jrivets/log4g.svg?branch=master)](https://travis-ci.org/jrivets/log4g)

Log4g is an open source project which intends to bring fast, flexible and scalable logging solution to the Golang world. It allows the developer to control which log statements are output with arbitrary granularity. It is fully configurable at runtime using external configuration files. Among other logging approaches log4g borrows some cool stuff from log4j library, but it doesn't try to repeat the library architecture, even many things look pretty similar (including the name of the project with one letter different only)

## Quick start
To start working with log4g you have to import log4g package, get a `Logger` instance and start to use it. log4g will use default configuration if no configuration is provided:

```
import "github.com/jrivets/log4g"
func main() {
	helloLogger := log4g.GetLogger("Hello")
	helloLogger.Info("Hello ", "GoLang World")
	defer log4g.Shutdown()
}

```

log4g uses concurrent logging messages processing with active usage of go routines. In addition, some internal components, like plugable appenders, may require strong initialization - finalization lifecycle. To avoid data loss, finalize, and free  resources properly the `log4g.Shutdown()` function should be called. It can be called just before your application is over, otherwise you probably will not have a chance to see some of your logging messages. 

## Architecture
log4g operates with the following terms and components: _log level_,  _logger_, _log event_, _logger name_, _logger context_, _appender_, and _log4g configuration_. 
`log4g.go` defines types, interfaces and public functions that allow to configure, use and expand the library functionalily. 

### Log Level
_Log level_ is an integer value which lies in [0..70] range. Every log message is associated with a certain _log level_ when the message is emitted. Depending on log4g configuration some messages can be filtered out and skipped from the processing flow because of their level. A level with lowest value has higher priority than the level with highest value. This means that if level X is set as maximum allowed, only messages with levels X1 <= X will be processed.

There are 7 _log level_ constants are pre-defined:
 * FATAL = 10
 * ERROR = 20
 * WARN = 30
 * INFO = 40
 * DEBUG = 50
 * TRACE = 60
 * ALL = 70

Any _log level_ value is associated with its name. Log message with certain _log level_ can be eventually formatted so the _log level_ name will be placed in the logging statement for the message. The pre-defined _log level_ values have the similar as the constat names associations (FATAL, ERROR etc.). Users can define their own _log levels_ names or change the pre-defined ones. To do this, users should make appropriate settings in _log4g configuration_ configuration file or do it programmatically, for example:

```
    ok := log4g.SetLogLevelName(23, "SEVERE")
```

In the example above the _log level_ with value 23 is named like "SEVERE". All messages emitted with the log level 23 will be named as "SEVERE" in the final log text, if the message formatting suppose to show messages log levels.
 
### Logger Name
The first and foremost advantage of log4g resides in its ability to disable certain log statements while allowing others to print unhindered. This capability assumes that the logging space, that is, the space of all possible logging statements, is categorized according to some developer-chosen criteria.

_Logger name_ is a string which should start from a letter, can contain letters `[A-Za-z]`,
digits `[0-9]` and dots `.`. The name cannot have `.` as a last symbol. The _root logger name_ is an empty string `""`. 

_Logger names_ are case-sensitive and they follow the hierarchical naming rule: A _logger name_ is said to be an ancestor of another _logger name_ if its name followed by a dot is a prefix of the descendant _logger name_. A _logger name_ is said to be a parent of a child _logger name_ if there are no ancestors between itself and the descendant _logger name_.

For example, the _Logger names_ `FileSystem` is a parent of the `FileSystem.ntfs`. Similarly, `a.b` is a parent of `a.b.c` and an ancestor of `a.b.c.d`. This naming scheme should be familiar to most developers and especially log4j users.

### Log Level Settings
log4g log level filtering configuration consists of list of pairs *{logger name : maximum allowed log level}*. Hierarchical relations of logger names allow to build flexible and advanced filtering configurations.

Adding or changing value in the list can be done by calling `SetLogLevel()` function declared in log4g package:

```
    func SetLogLevel(loggerName string, level Level)
```

Every logging message level is checked against nearest ancestor of the message logger name from the list. If the logger name level is low than the message level, the message will be skipped and not processed further. 

For example, lets suppose that log levels were set for 2 log names:

```
    log4g.SetLogLevel("FileSystem", INFO)
    log4g.SetLogLevel("FileSystem.ntfs", DEBUG)
```

which produce 2 pairs in the log level settings list. Logging messages made for `FileSystem.ext2` logger name will be filtered by `INFO` level, because the nearest ancestor for the name is `FileSystem`. But logging messages made for `FileSystem.ntfs` of `FileSystem.ntfs.hidden` will be filtered by `DEBUG` level, because the nearest ancestor for the namea is `FileSystem.ntfs`. 

log4g always has _root log level setting_ configured for _root logger name_

### Logger
`Logger` is an interface which allows to post logging messages to log4g for further processing. The instance of the interface can be retrieve by the function:

```
    func GetLogger(loggerName string) Logger
```

The function is idempotent, so it always returns same object for the same _logger name_ regardless of the log4g configuration and settings were made between different function calls. 

The `Logger` interface is only one "front end" element of the logging message processing, all logging messages in log4g are submitted through instances of the interface. 

Any logging message, submitted to log4g via `Logger`, is checked against _Log Level Settings_ and if the message should NOT be filtered because of its level, it is transformed to `Event` object which is passed to _Logger Context_ for further processing. 

### Logger Context
_Logger Context_ is an internal component which allows to aggregate logging messages from different _loggers_ and distribute them between _Appenders_ associated with the _Logger Context_. 

There is no special functions to configure _Logger Contexts_, so users can specify their configurations via log4g configuration functions calls. 

Every _Logger_ is associated with one _Logger Context_, but one _Logger Context_ can be associated with multiple _Loggers_. This association is done by same manner how _Log Level Settings_ are applied to _Loggers_: every _Logger Context_ and _Logger_ are named objects, so they "Logger Names" associated with them. _Logger Context_ is associated with a _Logger_ if its logger name is closed ancestor for the _Logger_ name. 

log4g always has _Logger Context_ associated with _root logger name_, so every _Logger_ will always be associated with at least this _root Logger Context_.

### Appender
log4g allows configurations when logging message will be sent to multiple destinations. The component which is plugged to log4g and implements a destination specific is called _Appender_. From log4g perspective every _appender_ implements `log4g.Appender` interface. Different _appenders_ can have different configurations based on the implementation specific. An _appender_ can be associated with multiple _Logger Contexts_ to have an ability to receive logging messages from different _loggers_.

_Appender_ is uniquely named structure, it means at a moment of time there could be only one appender instance with a certain name. Every _appender_ belongs to a specific appender type, which is identified by name. log4g allows to have many _appenders_ with the same type configured. In default configuration there are 2 types of appenders allowed - `log4g/consoleAppender` and `log4g/fileAppender`. Users can implement their own _appenders_ for a destination specific, register them in log4g, and make Events be sent to them by providing appropriate configuration.

### Log4g Configuration
log4g initialized in default configuration, so to start to use developers just can receive a _logger_ and starts to send messages into it:

```
        log4g.GetLogger("Hello").Info("Hello ", "GoLang logging world!")
        ...
        log4g.Shutdown()
```

what should cause the output like this into the console:

```
[09-19 10:03:53.487] INFO  Hello: Hello GoLang logging world!
```
but the console could be not only place where the logging messages should come to, so users have an ability to provide configuration as a `map[string]string` object representing a set of _{key: value}_ configuration pairs:

```
    func Config(props map[string]string) error
```

Another function allows to read configuration from file:

```
    func ConfigF(configFileName string) error
```

Both of this functions can be called multiple times in the program run-time, what will cause of re-configuring log4g in case of correct configuration is provided.

log4g configuration is provided like a set of _{key:value}_ pairs. The set of pairs can be specified in `map[string]string` object or by a property file. The property file is a text file with the following agreements:
* The _{key:value}_ pair is specified in one line as <key>=<value> form
* empty lines are ignored
* lines with first `#` symbol are considered like comments

Every configuration key has _{object}.{name}.{object param}_ format. For example `context.FileSystem.ntfs.buffer` key has:
* {object} == context
* {name} == FileSystem.ntfs
* {object param} == buffer

The following types of objects are supported:
* **level**
* **appender**
* **context**
* **logger**

#### level configuration
The **level** object can be configured like:

```
level.12=SEVERE
```

which allows to associate _logger level_ with its name. The _object param_ is an integer in the rang [0..70], no restrictions on its value is applied.

#### appender configuration
The **appender** object can be configured like:

```
appender.root.type=log4g/consoleAppender
appender.file.type=log4g/fileAppender
```

This configuration defines 2 appenders with names "root" and "file" respectively. For both appenders "type" parameter is specified. 

There are 2 appenders which come with log4g: console and file appenders. For both of them **layout** parameter must be defined. The appender **layout** value defines the output format of logging message. The **layout** value is a text with placeholders as follows:
*  **%c** - logger name
*  **%d{date/time format}** - date/time. The date/time format should be specified in time.Format() form like "Mon, 02 Jan 2006 15:04:05 -0700" etc.
* **%p** - log level name
* **%m** - the logging message text 
* **%%** - `%` symbol

#### context configuration
The **context** object can be configured like:

```
context.buffer=100
context.FileSystem.ntfs.appenders=root,file
```

First **context** has empty _{name}_ which means that the context is applied for _root logger name_. Second context specified for "FileSystem.ntfs" logger name.

#### logger configuration
The **logger** object can be configured like:

```
logger.level=INFO
logger.FileSystem.ntfs.level=DEBUG
```

Like for **context** object the _{name}_ field specifies the logger name. First line sets log level to INFO for _root logger name_. Second line sets DEBUG level for "FileSystem.ntfs" logger name.

All supported parameters listed in the following example:

```
# Associate log levels with their names
level.10=FATAL
level.11=SEVERE

# Console appender
appender.console.type=log4g/consoleAppender
appender.console.layout=%p %m 

# File appender
appender.file.type=log4g/fileAppender
appender.file.layout=[%d{01-02 15:04:05.000}] %p %c: %m 
appender.file.fileName=console.log

# append parameter defines whether new messages will be added 
# to the log file, or previous context will be lost
appender.file.append=false

# maxFileSize limits the maximum file size (see rotate parameter). 
# The value can be specified as human-readable form 10M, 2Gib etc.
appender.file.maxFileSize=20000

# maxLines limits maximum number of lines written to the file 
appender.file.maxLines=2000

# rotate defines file rotation policy: 
# "none" - no rotation happens, the log file will grow with no limits
# "size" - logging message will be written to new file, 
#          if file size or number of lines exceeds maximum values
# "daily" - same like "size" + new file is created on daily basis
#          even if limits are not reached.
appender.file.rotate=daily 

# Logger Context for root logger name
context.appenders=console

# buffer specifies the size of channel (Log events) between loggers and appenders
context.buffer=100

# blocking specifies the context behaviour in case of the event channel is full.
# if it is true (default value) then the logger call will be blocked 
# until it can put log event to the channel.
# if it is false, logger is never blocked, but the log event will be lost if the channel is full.
context.blocking=false

# level specifies log level for the context log name (root in this case)
context.level=DEBUG

# this context defined for "a.b" logger name will send log events to 2 appenders
context.a.b.appenders=console,file 

# level - specifies log level for the logger name "a.b.c.d"
logger.a.b.c.d.level=TRACE
```

## Implementing Appenders
TBD.



