package log4g

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jrivets/gorivets"
)

// log4g the appender registration name
const fileAppenderName = "log4g/fileAppender"

// layout - appender setting to specify format of the log event to message transformation
// This param must be provided when new appender is created
const FAParamLayout = "layout"

// fileName - specifies fileName where log message will be written to
// This param must be provided when new appender is created
const FAParamFileName = "fileName"

// append - appender setting if it is set to true the new log messages will be added to
// the end of file appending them to the current content of the file.
// this parameter is OPTIONAL, default value is true
const FAParamFileAppend = "append"

// buffer - appender settings which allows to set the size of log event buffer
// this parameter is OPTIONAL, default value is 100
const FAParamFileBuffer = "buffer"

// maxFileSize - appender settings which limits the size of the file chunk. The size can be specified in
// human readable form like 10Mb or 1000kiB etc. 1000 <= maxFileSize <= maxInt64,
// but should be less at than maxDiskSpace/2 if rotate != none!
// this parameter is OPTIONAL, default value is maxInt64. It is ignored if rotate == none
const FAParamMaxSize = "maxFileSize"

// maxDiskSpace - appender settings which limits the total disk space required
// for all log chunks. The number can be specified in human readable form like
// 10Mb or 400kB etc. 2000 <= maxDiskSpace <= maxInt64
// this parameter is OPTIONAL, default value is maxInt64. It is ignored if
// rotate == none.
const FAParamMaxDiskSpace = "maxDiskSpace"

// rotate - defines file-chunks rotation policy.
// this parameter is OPTIONAL, default value is none.
const FAParamRotate = "rotate"

// possible values of rotate param
// none: no rotation at all
// size: just rotate if maxFileSize OR maxLines is reached
// daily: rotate every new day (the host time midnight) or maxFileSize OR maxLines is reached
var rotateState = map[string]int{"none": rsNone, "size": rsSize, "daily": rsDaily}

const (
	rsNone = iota
	rsSize
	rsDaily
)

type fileAppenderFactory struct {
}

type fileAppender struct {
	msgChannel     chan string
	controlCh      chan bool
	fileName       string
	file           *os.File
	layoutTemplate LayoutTemplate
	fileAppend     bool
	maxSize        int64
	maxDiskSpace   int64
	rotate         int
	stat           stats
}

type stats struct {
	chunks     *gorivets.SortedSlice
	chunksSize int64

	size          int64
	startTime     time.Time
	lastErrorTime time.Time
}

type chunkInfo struct {
	id   int
	name string
	size int64
}

func (ci *chunkInfo) Compare(other gorivets.Comparable) int {
	return ci.id - other.(*chunkInfo).id
}

var faFactory *fileAppenderFactory

func init() {
	faFactory = &fileAppenderFactory{}
	RegisterAppender(faFactory)
}

// The factory allows to create an appender instances
func (faf *fileAppenderFactory) Name() string {
	return fileAppenderName
}

func (faf *fileAppenderFactory) NewAppender(params map[string]string) (Appender, error) {
	layout, ok := params[FAParamLayout]
	if !ok || len(layout) == 0 {
		return nil, errors.New("Cannot create file appender: layout should be specified")
	}

	fileName, ok := params[FAParamFileName]
	if !ok || len(fileName) == 0 {
		return nil, errors.New("Cannot create file appender: file should be specified")
	}

	layoutTemplate, err := ParseLayout(layout)
	if err != nil {
		return nil, errors.New("Cannot create file appender, incorrect layout: " + err.Error())
	}

	buffer, err := gorivets.ParseInt(params[FAParamFileBuffer], 1, 10000, 100)
	if err != nil {
		return nil, errors.New("Invalid " + FAParamFileBuffer + " value: " + err.Error())
	}

	fileAppend, err := gorivets.ParseBool(params[FAParamFileAppend], true)
	if err != nil {
		return nil, errors.New("Invalid " + FAParamFileAppend + " value: " + err.Error())
	}

	maxFileSize, err := gorivets.ParseInt64(params[FAParamMaxSize], 1000, maxInt64, maxInt64)
	if err != nil {
		return nil, errors.New("Invalid " + FAParamMaxSize + " value: " + err.Error())
	}

	maxDiskSpace, err := gorivets.ParseInt64(params[FAParamMaxDiskSpace], 2000, maxInt64, maxInt64)
	if err != nil {
		return nil, errors.New("Invalid " + FAParamMaxDiskSpace + " value: " + err.Error())
	}

	rotateStr, ok := params[FAParamRotate]
	rotateStr = strings.Trim(rotateStr, " ")
	rState := rsNone
	if ok && len(rotateStr) > 0 {
		rState, ok = rotateState[rotateStr]
		if !ok {
			return nil, errors.New("Unknown rotate state \"" + rotateStr +
				"\", expected \"none\", \"size\", or \"daily \" value")
		}
	}

	if maxDiskSpace/2 < maxFileSize && rState != rsNone {
		return nil, errors.New("Invalid " + FAParamMaxDiskSpace +
			" value. It should be at least twice bigger than " + FAParamMaxSize)
	}

	app := &fileAppender{}
	app.msgChannel = make(chan string, buffer)
	app.controlCh = make(chan bool, 1)
	app.layoutTemplate = layoutTemplate
	app.fileName = fileName
	app.fileAppend = fileAppend
	app.maxSize = maxFileSize
	app.maxDiskSpace = maxDiskSpace
	app.rotate = rState
	app.stat.chunks, app.stat.chunksSize = app.getLogChunks()

	go func() {
		defer app.close()
		app.stat.startTime = time.Now()
		for {
			str, ok := <-app.msgChannel
			if !ok {
				break
			}

			if app.isRotationNeeded() {
				app.rotateFile()
			}
			app.writeMsg(str)
		}
	}()
	return app, nil
}

func (faf *fileAppenderFactory) Shutdown() {
	// do nothing here, appenders should be shut down by log context
}

func (fa *fileAppender) Append(event *Event) (ok bool) {
	ok = false
	defer gorivets.EndQuietly()
	msg := ToLogMessage(event, fa.layoutTemplate)
	fa.msgChannel <- msg
	ok = true
	return ok
}

func (fa *fileAppender) Shutdown() {
	close(fa.msgChannel)
	<-fa.controlCh
}

func (fa *fileAppender) rotateFile() error {
	fa.archiveCurrent()

	fa.stat.size = 0
	fa.stat.startTime = time.Now()

	flags := os.O_WRONLY | os.O_CREATE
	if fa.fileAppend {
		flags = os.O_WRONLY | os.O_APPEND | os.O_CREATE
		if fInfo, err := os.Stat(fa.fileName); err == nil {
			fa.stat.size = fInfo.Size()
		}
	}

	fd, err := os.OpenFile(fa.fileName, flags, 0660)
	if err != nil {
		panic("File Appender cannot open file " + fa.fileName + " to store logs: " + err.Error())
	}
	fa.file = fd

	return nil
}

func (fa *fileAppender) archiveCurrent() {
	// if there is no file, or it is the first visit of the method for the appender
	// and we would like to continue write to the same file...
	finfo, err := os.Stat(fa.fileName)
	if err != nil || (fa.file == nil && fa.fileAppend) {
		return
	}

	archiveName, _ := filepath.Abs(fa.fileName)
	if fa.rotate == rsDaily {
		archiveName += "." + finfo.ModTime().Format("2006-01-02")
	}

	id := 1
	if fa.stat.chunks.Len() > 0 {
		id = fa.stat.chunks.At(fa.stat.chunks.Len()-1).(*chunkInfo).id + 1
	}

	if fa.file != nil {
		fa.file.Close()
		fa.file = nil
	}
	archiveName = archiveName + "." + strconv.Itoa(id)
	err = os.Rename(fa.fileName, archiveName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "File appender %+v: it is impossible to rename file \"%s\" to \"%s\": %s\n", fa, fa.fileName, archiveName, err)
		return
	}

	fa.stat.chunks.Add(&chunkInfo{id, archiveName, fa.stat.size})
	fa.stat.chunksSize += fa.stat.size
}

func (fa *fileAppender) cutChunks() {
	if fa.rotate == rsNone {
		return
	}
	for (fa.stat.chunksSize+fa.stat.size) > fa.maxDiskSpace && fa.stat.chunks.Len() > 0 {
		chunk := fa.stat.chunks.DeleteAt(0).(*chunkInfo)
		fa.stat.chunksSize -= chunk.size
		if err := os.Remove(chunk.name); err != nil {
			fmt.Fprintf(os.Stderr, "Could not remove chunk %s, err=%s", chunk.name, err)
		}
	}
}

func (fa *fileAppender) getLogChunks() (*gorivets.SortedSlice, int64) {
	archiveName, _ := filepath.Abs(fa.fileName)
	nameRegExp := archiveName + "\\.\\d+"
	if fa.rotate == rsDaily {
		nameRegExp = archiveName + "\\.\\d{4}-\\d{2}-\\d{2}\\.\\d+"
	}

	dir := filepath.Dir(archiveName)
	fileInfos, _ := ioutil.ReadDir(dir)

	chunks, _ := gorivets.NewSortedSlice(5)
	var size int64 = 0
	for _, fInfo := range fileInfos {
		if m, _ := regexp.MatchString(nameRegExp, fInfo.Name()); fInfo.IsDir() || !m {
			continue
		}

		idx := strings.LastIndex(fInfo.Name(), ".")
		if idx < 0 {
			continue
		}

		fId, err := strconv.Atoi(fInfo.Name()[idx+1:])
		if err != nil {
			continue
		}
		chunks.Add(&chunkInfo{fId, fInfo.Name(), fInfo.Size()})
		size += fInfo.Size()
	}
	return chunks, size
}

func (fa *fileAppender) isRotationNeeded() bool {
	if fa.file == nil {
		return true
	}

	switch fa.rotate {
	case rsNone:
		return false
	case rsSize:
		return fa.sizeRotation()
	case rsDaily:
		return fa.sizeRotation() || fa.timeRotation()
	}
	return false
}

func (fa *fileAppender) sizeRotation() bool {
	return fa.stat.size > fa.maxSize
}

func (fa *fileAppender) timeRotation() bool {
	now := time.Now()
	return fa.stat.startTime.Day() != now.Day() || now.Sub(fa.stat.startTime) > time.Hour*24
}

func (fa *fileAppender) writeMsg(msg string) {
	n, err := fmt.Fprint(fa.file, msg, "\n")

	if err != nil {
		if time.Since(fa.stat.lastErrorTime) > time.Minute {
			fa.stat.lastErrorTime = time.Now()
			fmt.Fprintf(os.Stderr, "File appender %+v: %s\n", fa, err)
		}
		return
	}

	fa.stat.size += int64(n)
	fa.cutChunks()
}

func (fa *fileAppender) close() {
	err := recover()
	if err != nil {
		fmt.Fprintf(os.Stderr, "File appender %+v: %s\n", fa, err)
	}
	if fa.file != nil {
		fa.file.Sync()
		fa.file.Close()
		fa.file = nil
	}
	fa.controlCh <- true
	close(fa.controlCh)
}
