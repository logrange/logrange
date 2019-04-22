package log4g

import (
	"bytes"
	"errors"
	"fmt"
)

// layout pieces types
const (
	lpText = iota
	lpLoggerName
	lpDate
	lpLogLevel
	lpMessage
	lpLoggerId
)

// parse states
const (
	psText = iota
	psPiece
	psDateStart
	psDate
)

type layoutPiece struct {
	value     string
	pieceType int
}

type LayoutTemplate []layoutPiece

var logLevelNames []string

func init() {
	logLevelNames = lm().config.levelNames
}

// ParseLayout parses the layout parameter and returns LayoutTemplate instance
// if it is correct.
// The log message layout is format string with the following placeholders:
// %c - logger name
// %d{date/time format} - date/time. The date/time format should be specified
//				in time.Format() form like "Mon, 02 Jan 2006 15:04:05 -0700"
// %p - priority name
// %m - the log message
// %i - the logger identifier, if it is set up
// %% - '%'
//
// For example, layout string '[%d{01-02 15:04:05.000}] %p %c: %m' will be parsed
// to a layout template, which can be used by ToLogMessage() to form the log
// line. For logger 'a.b.c' will produce message like this:
//
// [09-17 12:34:12.123] INFO a.b.c: Hello logging world!
//
func ParseLayout(layout string) (LayoutTemplate, error) {
	layoutTemplate := make(LayoutTemplate, 0, 10)
	state := psText
	startIdx := 0
	for i, rune := range layout {
		switch state {
		case psText:
			if rune == '%' {
				layoutTemplate = addPiece(layout[startIdx:i], lpText, layoutTemplate)
				state = psPiece
			}
		case psPiece:
			state = psText
			startIdx = i + 1
			switch rune {
			case 'c':
				layoutTemplate = addPiece("c", lpLoggerName, layoutTemplate)
			case 'd':
				state = psDateStart
			case 'p':
				layoutTemplate = addPiece("p", lpLogLevel, layoutTemplate)
			case 'm':
				layoutTemplate = addPiece("m", lpMessage, layoutTemplate)
			case 'i':
				layoutTemplate = addPiece("i", lpLoggerId, layoutTemplate)
			case '%':
				startIdx = i
			default:
				return nil, errors.New("Unknown layout identifier " + string(rune))
			}
		case psDateStart:
			if rune != '{' {
				return nil, errors.New("%d should follow by date format in braces like this: %d{...}, but found " + string(rune))
			}
			startIdx = i + 1
			state = psDate
		case psDate:
			if rune == '}' {
				layoutTemplate = addPiece(layout[startIdx:i], lpDate, layoutTemplate)
				state = psText
				startIdx = i + 1
			}
		}
	}

	if state != psText {
		return nil, errors.New("Unexpected end of layout, cannot parse it properly")
	}

	return addPiece(layout[startIdx:len(layout)], lpText, layoutTemplate), nil
}

// ToLogMessage transforms logEvent value to a log text line using provided
// layout template.
//
// For example, for the template '[%d{01-02 15:04:05.000}] %p %c: %m'
// the code line 'abcLogger.info("Hello logging world!")' will produce the log message:
//
// [09-17 12:34:12.123] INFO a.b.c: Hello logging world!
//
// for the logger 'a.b.c'
func ToLogMessage(logEvent *Event, template LayoutTemplate) string {
	buf := bytes.NewBuffer(make([]byte, 0, 64))

	for _, piece := range template {
		switch piece.pieceType {
		case lpText:
			buf.WriteString(piece.value)
		case lpLoggerName:
			buf.WriteString(logEvent.LoggerName)
		case lpDate:
			buf.WriteString(logEvent.Timestamp.Format(piece.value))
		case lpLogLevel:
			buf.WriteString(logLevelNames[logEvent.Level])
		case lpMessage:
			buf.WriteString(fmt.Sprint(logEvent.Payload))
		case lpLoggerId:
			if logEvent.Id != nil {
				buf.WriteString(fmt.Sprint(logEvent.Id))
			}
		}
	}
	return buf.String()
}

func addPiece(str string, pieceType int, template LayoutTemplate) LayoutTemplate {
	if len(str) == 0 {
		return template
	}
	return append(template, layoutPiece{str, pieceType})
}
