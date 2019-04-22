package log4g

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/jrivets/gorivets"
)

const consoleAppenderName = "log4g/consoleAppender"

// layout - appender setting to specify format of the log event to message
// transformation
const CAParamLayout = "layout"

// async - defines whether the appender will block the logging call, or do
// it asynchronously
const CAParamAsync = "async"

type consoleAppender struct {
	layoutTemplate LayoutTemplate
	async          bool
}

type consoleAppenderFactory struct {
	msgChannel chan string
	out        io.Writer
}

var caFactory *consoleAppenderFactory

func init() {
	caFactory = &consoleAppenderFactory{make(chan string, 1000), os.Stdout}

	err := RegisterAppender(caFactory)
	if err != nil {
		close(caFactory.msgChannel)
		fmt.Println("It is impossible to register console appender: ", err)
		panic(err)
	}
	go func() {
		for {
			str, ok := <-caFactory.msgChannel
			if !ok {
				break
			}
			caFactory.write(str)
		}
	}()
}

func (f *consoleAppenderFactory) write(str string) {
	fmt.Fprint(f.out, str, "\n")
}

func (*consoleAppenderFactory) Name() string {
	return consoleAppenderName
}

func (caf *consoleAppenderFactory) NewAppender(params map[string]string) (Appender, error) {
	layout, ok := params[CAParamLayout]
	if !ok || len(layout) == 0 {
		return nil, errors.New("Cannot create console appender without specified layout")
	}

	layoutTemplate, err := ParseLayout(layout)
	if err != nil {
		return nil, errors.New("Cannot create console appender: " + err.Error())
	}

	asyncParam, ok := params[CAParamAsync]
	var async bool = false
	if ok && asyncParam != "" {
		pasync, err := gorivets.ParseBool(asyncParam, true)
		if err != nil {
			async = pasync
		}
	}

	return &consoleAppender{layoutTemplate, async}, nil
}

func (caf *consoleAppenderFactory) Shutdown() {
	close(caf.msgChannel)
	caFactory.msgChannel = nil
}

// Appender interface implementation
func (cAppender *consoleAppender) Append(event *Event) (ok bool) {
	ok = false
	defer gorivets.EndQuietly()
	msg := ToLogMessage(event, cAppender.layoutTemplate)
	if cAppender.async {
		caFactory.msgChannel <- msg
	} else {
		caFactory.write(msg)
	}
	ok = caFactory.msgChannel != nil
	return ok
}

func (cAppender *consoleAppender) Shutdown() {
	// Nothing should be done for the console appender
}
