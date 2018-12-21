package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/logrange/logrange/pkg/collector/scanner/parser/date"
	"github.com/logrange/logrange/pkg/collector/scanner/parser/logs"
	"os"
	"regexp"
	"regexp/syntax"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logrange/logrange/pkg/collector/model"
	"github.com/logrange/logrange/pkg/records"
	"github.com/logrange/logrange/pkg/util"

	"github.com/jrivets/log4g"
	"github.com/mohae/deepcopy"
)

type (
	// Config is a structure which contains scanner configuration and settings
	Config struct {
		// ScanPaths contains a list of path that should be scanned and check
		// for potential sources. Scan Paths are defined like Globs, for example:
		// /var/log/*.log etc.
		ScanPaths            []string `json:"scanPaths"`
		ScanPathsIntervalSec int      `json:"scanPathsIntervalSec"`

		// Exclude contains slice of regular expressions (the slice could be empty)
		// which will be applied to files found by ScanPaths. The Exclude slice
		// of reg-exps intended to exclude some files which could be scanned.
		Exclude []string `json:"exclude"`

		// StateFlushIntervalSec defines the time interval to store the state
		StateFlushIntervalSec int `json:"stateFlushIntervalSec"`

		// FileFormats allows to specify which date-time formats can be used
		// when parsing files by the specific matcher
		FileFormats []*FileFormat `json:"fileFormats"`

		// RecordMaxSizeBytes defines a maximum size of one record
		RecordMaxSizeBytes int `json:"recordMaxSizeBytes"`

		// EventMaxRecords defines how many records can be packed in one event
		EventMaxRecords     int `json:"eventMaxRecords"`
		EventSendIntervalMs int `json:"eventSendIntervalMs"`
	}

	// FileFormat defines a pattern of file matching and time formats, if any,
	// that should be applied to the files when parsing
	FileFormat struct {
		// PathMatcher defines regExp which should be applied to a file name
		// to identify whether the time format below will be applied to it or not
		// example is '.*'
		PathMatcher string `json:"pathMatcher"`

		// DataFormat defines how data is formatted in the file: text, json etc.
		// By the field value a type of parser for the worker will be created then
		DataFormat string `json:"format"`

		// TimeFormats contains a list of supposed date-time formats that should
		// be tried for parsing the file lines (see parser.date) example is
		// ["MMM D, YYYY h:mm:ss P"]
		TimeFormats []string `json:"timeFormats"`
	}

	desc struct {
		Id   string `json:"id"`
		File string `json:"file"`

		// The Scan size is used to catch up if the file size was changed and
		// if it is reduced for later scans, the file was truncated and the
		// offset will be reset. Please see getDescsToScan()
		ScanSize int64 `json:"scanSize"`
		Offset   int64 `json:"offset"`
	}

	descs map[string]*desc

	workers map[string]*worker

	Scanner struct {
		cfg       *Config
		ctx       context.Context
		cancel    context.CancelFunc
		logger    log4g.Logger
		stStorage StateStorage

		events  chan *model.Event
		descs   atomic.Value
		workers atomic.Value

		// tracks list of files which were excluded for reporting purposes only
		// contains []string
		excludes atomic.Value
		stopWg   sync.WaitGroup
		lock     sync.Mutex
		wrkId    int32
	}

	CollectorStats struct {
		Config *Config
		// contains list of files that were excluded from processing
		Excludes []string
		Workers  []*WorkerStats
	}
)

const (
	cTxtDataFormat  = "text"
	cJsonDataFormat = "json"
)

func NewDefaultConfig() *Config {
	return &Config{
		ScanPaths:             []string{"/var/log/*.log", "/var/log/*/*.log"},
		ScanPathsIntervalSec:  5,
		StateFlushIntervalSec: 5,
		RecordMaxSizeBytes:    16 * 1024,
		EventMaxRecords:       1000,
		EventSendIntervalMs:   200,
	}
}

func (c *Config) String() string {
	return util.ToJsonStr(c)
}

func (c *Config) Apply(c1 *Config) {
	if c1 == nil {
		return
	}
	if len(c1.ScanPaths) != 0 {
		c.ScanPaths = deepcopy.Copy(c1.ScanPaths).([]string)
	}
	if c1.ScanPathsIntervalSec != 0 {
		c.ScanPathsIntervalSec = c1.ScanPathsIntervalSec
	}
	if c1.StateFlushIntervalSec != 0 {
		c.StateFlushIntervalSec = c1.StateFlushIntervalSec
	}
	if len(c1.FileFormats) != 0 {
		c.FileFormats = deepcopy.Copy(c1.FileFormats).([]*FileFormat)
	}
	if c1.RecordMaxSizeBytes != 0 {
		c.RecordMaxSizeBytes = c1.RecordMaxSizeBytes
	}
	if c1.EventMaxRecords != 0 {
		c.EventMaxRecords = c1.EventMaxRecords
	}
	if c1.EventSendIntervalMs != 0 {
		c.EventSendIntervalMs = c1.EventSendIntervalMs
	}
}

//=== fileFormat
func (c *FileFormat) String() string {
	return util.ToJsonStr(c)
}

//=== scanner
func NewScanner(cfg *Config, ss StateStorage) (*Scanner, error) {
	if err := checkCfg(cfg); err != nil {
		return nil, err
	}

	c := new(Scanner)
	c.cfg = cfg
	c.workers.Store(make(workers))
	c.descs.Store(newDescs())
	c.excludes.Store([]string{})
	c.logger = log4g.GetLogger("collector.scanner")
	c.stStorage = ss
	return c, nil
}

func (s *Scanner) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.ctx != nil && s.ctx.Err() == nil {
		return fmt.Errorf("wrong state, the component is already running")
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.events = make(chan *model.Event)

	s.logger.Info("Starting, config=", s.cfg)

	if err := s.loadStatesBeforeRun(); err != nil {
		s.logger.Warn("could not start collector, err=", err)
		s.stopInternal()
		return err
	}

	s.runScanPaths()
	s.runFlushState()

	s.logger.Info("Started!")
	return nil
}

func (s *Scanner) Stop() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Info("Stopping...")
	s.stopInternal()
	s.logger.Info("Stopped.")
	return nil
}

func (s *Scanner) stopInternal() {
	s.cancel()
	s.stopWg.Wait()
	close(s.events)
	s.ctx = nil
}

func (s *Scanner) Events() <-chan *model.Event {
	return s.events
}

func (s *Scanner) GetStats() *CollectorStats {
	cs := new(CollectorStats)
	cs.Config = new(Config)
	*cs.Config = *s.cfg
	cs.Excludes = s.excludes.Load().([]string)
	wkrs := s.workers.Load().(workers)
	cs.Workers = make([]*WorkerStats, 0, len(wkrs))
	for _, wkr := range wkrs {
		ws := wkr.GetStats()
		idx := sort.Search(len(cs.Workers), func(pos int) bool {
			return cs.Workers[pos].Filename >= ws.Filename
		})
		cs.Workers = append(cs.Workers, ws)
		if idx < len(cs.Workers)-1 {
			copy(cs.Workers[idx+1:], cs.Workers[idx:])
		}
		cs.Workers[idx] = ws

	}
	return cs
}

func (s *Scanner) loadStatesBeforeRun() error {
	s.updateWorkersList(newDescs())
	ds1, err := s.loadState()
	if err != nil {
		return err
	}
	ds2 := s.getDescsToScan()
	s.logger.Info("loadStatesBeforeRun: ", len(ds2), " files were found by scan and ", len(ds1), " files states were read from state file.")
	s.logger.Debug("loadStatesBeforeRun: From state file ", ds1)
	s.logger.Debug("loadStatesBeforeRun: Found by scan ", ds2)
	s.replaceOrRotate(ds1, ds2)
	s.updateWorkersList(ds2)
	s.logger.Info(len(ds2), " sources found, will scan them...")
	return nil
}

func (s *Scanner) runScanPaths() {
	s.stopWg.Add(1)
	go func() {
		s.logger.Info("Start scanning paths every ", s.cfg.ScanPathsIntervalSec, " seconds...")
		defer func() {
			s.logger.Info("Stop scanning paths.")
			s.stopWg.Done()
		}()

		ticker := time.NewTicker(time.Second *
			time.Duration(s.cfg.ScanPathsIntervalSec))

		for s.wait(ticker) {
			// existing descriptors are in ds1
			ds1 := s.descs.Load().(descs)
			// still found are in ds2
			ds2 := s.getDescsToScan()
			if chngs := s.replaceOrRotate(ds1, ds2); chngs > 0 {
				s.logger.Info("scan procedure: ", chngs, " changes were made. old descs=", len(ds1), ", new descs=", len(ds2))
			}
			s.updateWorkersList(ds2)
		}
	}()
}

func (s *Scanner) getWorkerConfig(d *desc) (*workerConfig, error) {
	wc := &workerConfig{
		desc:             d,
		EventMaxRecCount: s.cfg.EventMaxRecords,
	}
	wc.ctx, wc.cancel = context.WithCancel(s.ctx)
	wc.logger = log4g.GetLogger("collector.scanner.worker").
		WithId(fmt.Sprintf("{%d}", atomic.AddInt32(&s.wrkId, 1))).(log4g.Logger)

	df := getDataFormat(d, s.cfg.FileFormats)
	var err error
	switch df {
	case cJsonDataFormat:
		wc.parser, err = logs.NewK8sJsonParser(d.File, s.cfg.RecordMaxSizeBytes, wc.ctx)
	case "":
		fallthrough
	case cTxtDataFormat:
		dateParser := date.NewParser(getTimeFormats(d, s.cfg.FileFormats)...)
		wc.parser, err = logs.NewLineParser(d.File, dateParser, s.cfg.RecordMaxSizeBytes, wc.ctx)
	default:
		return nil, fmt.Errorf("Unsupported data format=%s expecting %s or %s", df, cTxtDataFormat, cJsonDataFormat)
	}
	return wc, err
}

func (s *Scanner) updateWorkersList(ds descs) {
	newWks := make(workers)
	oldWks := s.workers.Load().(workers)
	for id, d := range ds {
		w, ok := oldWks[id]

		if ok && d != w.desc {
			// descriptor was re-created, will re-scan
			w.Close()
		}

		if !ok || w.isClosed() {
			wc, err := s.getWorkerConfig(d)
			if err != nil {
				s.logger.Error("Could not create new worker by the descriptor ", d, ", err=", err)
				continue
			}

			w = newWorker(wc)
			s.stopWg.Add(1)
			go func(w *worker) {
				defer s.stopWg.Done()
				err := w.run(s.sendEvent)
				if err != nil {
					s.logger.Warn("Worker ", w.desc, " reports err=", err)
				}
				w.Close()
			}(w)
		}
		newWks[id] = w
	}

	for id, w := range oldWks {
		if _, ok := newWks[id]; !ok {
			if !w.isClosed() {
				w.stopWhenEof()
				newWks[id] = w
			}
		}
	}

	s.workers.Store(newWks)
	s.descs.Store(ds)
}

func (s *Scanner) sendEvent(ev *model.Event) error {
	select {
	case <-s.ctx.Done():
		return fmt.Errorf("Closed")
	case s.events <- ev:
	}
	return nil
}

func (s *Scanner) runFlushState() {
	s.stopWg.Add(1)
	go func() {
		s.logger.Info("Start flushing state every ", s.cfg.StateFlushIntervalSec, " seconds...")
		defer func() {
			s.logger.Info("Stop flushing state.")
			s.flushState()
			s.stopWg.Done()
		}()

		ticker := time.NewTicker(time.Second *
			time.Duration(s.cfg.StateFlushIntervalSec))

		for s.wait(ticker) {
			s.logger.Debug("Flushing state=", s.descs.Load())
			if err := s.flushState(); err != nil {
				s.logger.Error("Unable to flush state, cause=", err)
			}
		}
	}()
}

// flushState stores state of descs to disk. The method can be called from runFlushState()
// only
func (s *Scanner) flushState() error {
	d := s.descs.Load().(descs)
	return s.saveState(d)
}

func (s *Scanner) wait(ticker *time.Ticker) bool {
	select {
	case <-s.ctx.Done():
		return false
	case <-ticker.C:
		return true
	}
}

func (s *Scanner) loadState() (descs, error) {
	s.logger.Info("Loading state from ", s.stStorage)

	res := newDescs()
	rb, err := s.stStorage.ReadData()
	if err != nil {
		if !os.IsNotExist(err) {
			s.logger.Warn("No status found err=", err, " stStorage=", s.stStorage)
			return res, err
		}
		return res, nil
	}

	if err = json.Unmarshal(rb, &res); err != nil {
		return nil, fmt.Errorf("cannot unmarshal state from %v; cause: %v", s.stStorage, err)
	}

	return res, nil
}

func (s *Scanner) saveState(dscs descs) error {
	data, err := json.Marshal(dscs)
	if err != nil {
		return fmt.Errorf("cannot marshal state=%v; cause: %v", dscs, err)
	}

	return s.stStorage.WriteData(data)
}

// getDescs scans paths and creates map of file desc(s) for all files matched in
// the paths
func (s *Scanner) getDescsToScan() descs {
	paths := s.cfg.ScanPaths

	res := make(descs)
	files := s.getFilesToScan(paths)
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			s.logger.Warn("Unable to get info for file=", f, ", skipping; cause: ", err)
			continue
		}

		id := util.GetFileId(f, info)
		res[id] = &desc{Id: id, File: f, ScanSize: info.Size(), Offset: 0}
	}

	if len(s.cfg.Exclude) > 0 {
		exclds := []string{}
		for _, ex := range s.cfg.Exclude {
			// ignoring error, cause we already tried it when checked the config
			re, _ := regexp.Compile(ex)
			for id, d := range res {
				if re.Match(records.StringToByteArray(d.File)) {
					delete(res, id)
					exclds = append(exclds, d.File)
				}
			}
		}
		s.excludes.Store(exclds)
	}
	return res
}

// replaceOrRotate iterates over new descriptors dssNew, which have been just
// found and tries to apply old state for them if it exists. If there is old
// state, but the file seems truncated, the offset will be reset.
// returns number of changes were made.
func (s *Scanner) replaceOrRotate(dssOld, dssNew descs) int {
	res := 0
	for id, ds := range dssNew {
		dsOld, ok := dssOld[id]
		if !ok {
			s.logger.Info("New descriptor is found ", ds)
			res++
			continue
		}

		if dsOld.ScanSize <= ds.ScanSize && dsOld.getOffset() <= ds.ScanSize {
			dssNew[id] = dsOld
			continue
		}

		res++
		s.logger.Info("Found descriptors inconsistency: old=", dsOld, " and the new one is ", ds, ". Seems like a rotation happens")
	}

	for id, dsOld := range dssOld {
		if _, ok := dssNew[id]; !ok {
			s.logger.Info("Seems like the file ", dsOld, " does not exist anymore. Forget about it.")
			res++
		}
	}
	return res
}

// getFilesToScan list of paths in GLOB (pathname patterns) style and turn
// them to real file doing the following actions:
// 1. Checks whether the file is readable (Stat could be taken)
// 2. Skip directories
func (s *Scanner) getFilesToScan(paths []string) []string {
	ep := util.ExpandPaths(paths)
	ff := make([]string, 0, len(ep))
	for _, p := range ep {
		var err error
		fin, err := os.Stat(p)
		if err != nil {
			s.logger.Warn("Skipping path=", p, "; cause: ", err)
			continue
		}

		if fin.IsDir() {
			s.logger.Warn("Skipping path=", p, "; cause: the path is directory")
			continue
		}

		ff = append(ff, p)
	}
	return util.RemoveDups(ff)
}

//=== descs
func newDescs() descs {
	return map[string]*desc{}
}

func (ds descs) MarshalJSON() ([]byte, error) {
	dl := make([]*desc, 0, len(ds))
	for _, d := range ds {
		dl = append(dl, d)
	}
	return json.Marshal(&dl)
}

func (ds descs) UnmarshalJSON(data []byte) error {
	dl := make([]*desc, 0, 5)
	err := json.Unmarshal(data, &dl)
	if err == nil {
		for _, d := range dl {
			ds[d.Id] = d
		}
	}
	return err
}

func (ds descs) String() string {
	return util.ToJsonStr(ds)
}

//=== desc

func (d *desc) MarshalJSON() ([]byte, error) {
	type alias desc
	return json.Marshal(&struct {
		*alias
		Offset int64 `json:"offset"`
	}{
		alias:  (*alias)(d),
		Offset: d.getOffset(),
	})
}

func (d *desc) addOffset(val int64) {
	atomic.AddInt64(&d.Offset, val)
}

func (d *desc) setOffset(val int64) {
	atomic.StoreInt64(&d.Offset, val)
}

func (d *desc) getOffset() int64 {
	return atomic.LoadInt64(&d.Offset)
}

func (d *desc) String() string {
	return util.ToJsonStr(d)
}

//=== helpers
func checkCfg(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("invalid config=%v", cfg)
	}
	if cfg.EventMaxRecords <= 0 {
		return fmt.Errorf("invalid config; eventMaxRecords=%v, must be > 0", cfg.EventMaxRecords)
	}
	if cfg.EventSendIntervalMs <= 100 {
		return fmt.Errorf("invalid config; eventSendIntervalMs=%v, must be > 100ms", cfg.EventSendIntervalMs)
	}
	if cfg.ScanPathsIntervalSec <= 0 {
		return fmt.Errorf("invalid config; scanPathsIntervalSec=%v, must be > 0sec", cfg.ScanPathsIntervalSec)
	}
	if cfg.StateFlushIntervalSec <= 0 {
		return fmt.Errorf("invalid config; stateFlushIntervalSec=%v, must be > 0sec", cfg.StateFlushIntervalSec)
	}
	if cfg.RecordMaxSizeBytes < 64 || cfg.RecordMaxSizeBytes > 32*1024 {
		return fmt.Errorf("invalid config; recordSizeMaxBytes=%v, must be in range [%v..%v]",
			cfg.RecordMaxSizeBytes, 64, 32*1024)
	}
	for _, f := range cfg.FileFormats {
		if err := checkFileFormat(f); err != nil {
			return fmt.Errorf("invalid config; invalid fileFormat=%v, %v", f, err)
		}
	}

	for _, ex := range cfg.Exclude {
		if _, err := regexp.Compile(ex); err != nil {
			return fmt.Errorf("invalid config; Could not compile regular expression in exclude: %s, err=%v", ex, err)
		}
	}
	return nil
}

func checkFileFormat(f *FileFormat) error {
	if strings.TrimSpace(f.PathMatcher) == "" {
		return errors.New("patchMatcher must be non-empty")
	}

	_, err := syntax.Parse(f.PathMatcher, syntax.Perl)
	if err != nil {
		return fmt.Errorf("pathMatcher is invalid; %v", err)
	}

	return nil
}

// getTimeFormats returns slice of Date-time formats found in between ff - FileFormat
// slice by matching filename with FileFormat regexps.
func getTimeFormats(d *desc, ff []*FileFormat) []string {
	fmts := []string{}
	for _, f := range ff {
		if match, _ := regexp.MatchString(f.PathMatcher, d.File); match {
			fmts = append(fmts, f.TimeFormats...)
		}
	}
	return fmts
}

// getDataFormat returns corresponding DataFromat (cTxtDataFormat, cJsonDataFormat etc.)
// by matching d.File with different FileFormats provided in ff slice
func getDataFormat(d *desc, ff []*FileFormat) string {
	for _, f := range ff {
		if match, _ := regexp.MatchString(f.PathMatcher, d.File); match {
			return f.DataFormat
		}
	}
	return cTxtDataFormat
}
