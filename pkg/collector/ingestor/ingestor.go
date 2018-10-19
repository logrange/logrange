// ingestor package contains a code which helps to build a data ingestor for the
// aggregator
package ingestor

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/pkg/collector/model"
	"github.com/logrange/logrange/pkg/dstruct"
	"github.com/logrange/logrange/pkg/logevent"
	"github.com/logrange/logrange/pkg/proto/atmosphere"
	"github.com/logrange/logrange/pkg/util"
	"regexp"
	"regexp/syntax"
	"strings"
	"sync"
	"time"

	"github.com/jrivets/log4g"
	"github.com/mohae/deepcopy"
	"github.com/pkg/errors"
)

type (
	// schema struct contains a schema descriptor, which includes the SchemaConfig
	// and corresponding reg-exp matcher, which is going to be used for matching
	// the schema. This structure is used internally for identifying the schema.
	schema struct {
		cfg     *SchemaConfig
		matcher *regexp.Regexp
	}

	// Ingestor is used for sending data received from scanner to an log aggregator.
	Ingestor struct {
		cfg        *Config
		aClient    atmosphere.Writer
		pktEncoder *encoder
		schemas    []*schema
		logger     log4g.Logger
		ctx        context.Context

		lock sync.Mutex
		// hdrsCache allows to cache headers by file name
		hdrsCache *dstruct.Lru
	}

	// hdrsCacheRec struct is used by Ingestor to cache association between file
	// name and the message data header.
	hdrsCacheRec struct {
		srcId string
		tags  logevent.TagLine
	}
)

func (hcr *hdrsCacheRec) String() string {
	return fmt.Sprint("{srcId=", hcr.srcId, ", tags=", hcr.tags, "}")
}

func NewIngestor(cfg *Config, ctx context.Context) (*Ingestor, error) {
	if err := checkConfig(cfg); err != nil {
		return nil, err
	}

	logger := log4g.GetLogger("collector.ingestor")
	logger.Info("Creating, config=", util.ToJsonStr(cfg))

	ing := new(Ingestor)
	ing.hdrsCache = dstruct.NewLru(10000, 5*time.Minute, nil)
	ing.cfg = deepcopy.Copy(cfg).(*Config)
	ing.pktEncoder = newEncoder()
	ing.logger = logger
	ing.ctx = ctx

	ing.schemas = make([]*schema, 0, len(cfg.Schemas))
	for _, s := range cfg.Schemas {
		ing.schemas = append(ing.schemas, newSchema(s))
	}

	logger.Info("Created!")
	return ing, nil
}

func (i *Ingestor) Run(ctx context.Context, events <-chan *model.Event) chan bool {
	i.connect()
	done := make(chan bool)
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				var err error
				for ctx.Err() == nil {
					if err == nil {
						err = i.ingest(ev)
						if err == nil {
							ev.Confirm()
							break
						}
					}
					i.logger.Info("Ingestor error, recovering; cause: ", err)
					err = i.connect()
				}
			}
		}
	}()
	return done
}

func (i *Ingestor) GetKnownTags() map[interface{}]interface{} {
	i.lock.Lock()
	res := i.hdrsCache.GetData()
	i.lock.Unlock()
	return res
}

func (i *Ingestor) IsConnected() bool {
	if i.aClient != nil {
		return true
	}
	return false
}

func (i *Ingestor) connect() error {
	i.logger.Info("Connecting to ", i.cfg.Server)
	var (
		acl atmosphere.Writer
		err error
	)

	retry := time.Duration(i.cfg.RetrySec) * time.Second
	for {
		acl, err = atmosphere.NewClient(i.cfg.Server,
			&atmosphere.ClientConfig{
				HeartBeatMs: i.cfg.HeartBeatMs,
				AccessKey: i.cfg.AccessKey,
				SecretKey: i.cfg.SecretKey})
		if err == nil {
			break
		}

		i.logger.Warn("Could not connect to the server, err=", err, " will try in ", retry)
		select {
		case <-i.ctx.Done():
			return fmt.Errorf("interrupted")
		case <-time.After(retry):
		}
		i.logger.Warn("after 5 sec")
	}

	i.aClient = acl
	i.logger.Info("connected")
	return nil
}

func (i *Ingestor) ingest(ev *model.Event) error {
	if i.aClient == nil {
		return fmt.Errorf("not initialized")
	}

	header, err := i.getHeaderByFilename(ev.File)
	if err != nil {
		i.aClient = nil
		return err
	}

	i.logger.Trace("Ingest header=", header, ", ev.File=", ev.File, ", len(ev.Records)=", len(ev.Records))
	buf, err := i.pktEncoder.encode(header, ev)
	if err != nil {
		i.aClient = nil
		return err
	}
	_, err = i.aClient.Write(buf, nil)
	if err != nil {
		i.aClient = nil
		return err
	}
	return nil
}

// getHeaderByFilename get filename and forms header using schema and configuration
// it can cache already calculated headers, so will work quickly this case
func (i *Ingestor) getHeaderByFilename(filename string) (*hdrsCacheRec, error) {
	i.lock.Lock()
	defer i.lock.Unlock()

	val := i.hdrsCache.Get(filename)
	if val != nil {
		hdr := val.Val().(*hdrsCacheRec)
		return hdr, nil
	}

	schm := i.getSchema(filename)
	if schm == nil {
		return nil, errors.New("no schema found!")
	}

	vars := schm.getVars(filename)
	tags := make(map[string]string, len(schm.cfg.Tags))

	for k, v := range schm.cfg.Tags {
		tags[k] = schm.subsVars(v, vars)
	}

	srcId := schm.subsVars(schm.cfg.SourceId, vars)
	tm, err := logevent.NewTagMap(tags)
	if err != nil {
		return nil, err
	}
	hdr := &hdrsCacheRec{srcId, tm.BuildTagLine()}

	i.hdrsCache.Put(filename, hdr, 1)
	return hdr, nil
}

func (i *Ingestor) getSchema(filename string) *schema {
	for _, s := range i.schemas {
		if s.matcher.MatchString(filename) {
			return s
		}
	}
	return nil
}

func (i *Ingestor) close() {
	i.logger.Info("Closing...")
	if i.aClient != nil {
		i.aClient.Close()
	}
	i.logger.Info("Closed.")
}

//=== schemaConfig

func (s *SchemaConfig) String() string {
	return util.ToJsonStr(s)
}

//=== schema

func newSchema(cfg *SchemaConfig) *schema {
	return &schema{
		cfg:     cfg,
		matcher: regexp.MustCompile(cfg.PathMatcher),
	}
}

func (s *schema) getVars(l string) map[string]string {
	names := s.matcher.SubexpNames()
	match := s.matcher.FindStringSubmatch(l)

	if len(names) > 1 {
		names = names[1:] //skip ""
	}
	if len(match) > 1 {
		match = match[1:] //skip "" value
	}

	vars := make(map[string]string, len(names))
	for i, n := range names {
		if len(match) > i {
			vars[n] = match[i]
		} else {
			vars[n] = ""
		}
	}
	return vars
}

func (s *schema) subsVars(l string, vars map[string]string) string {
	for k, v := range vars {
		l = strings.Replace(l, "{"+k+"}", v, -1)
	}
	return l
}

func (s *schema) String() string {
	return util.ToJsonStr(s.cfg)
}

//=== helpers

func checkConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("invalid config=%v", cfg)
	}

	if cfg.RetrySec < 1 {
		return fmt.Errorf("invalid config; retry connect timeout=%d, expecting 1 second or more", cfg.RetrySec)
	}

	if strings.TrimSpace(cfg.Server) == "" {
		return fmt.Errorf("invalid config; server=%v, must be non-empty", cfg.Server)
	}

	if cfg.HeartBeatMs < 100 {
		return fmt.Errorf("invalid config; heartBeatMs=%v, must be >= 100ms", cfg.HeartBeatMs)
	}

	if cfg.PacketMaxRecords <= 0 {
		return fmt.Errorf("invalid config; packetMaxRecords=%v, must be > 0", cfg.PacketMaxRecords)
	}

	if len(cfg.Schemas) == 0 {
		return errors.New("invalid config; at least 1 schema must be defined")
	}

	for _, s := range cfg.Schemas {
		if err := checkSchema(s); err != nil {
			return fmt.Errorf("invalid config; invalid schema=%v, %v", s, err)
		}
	}
	return nil
}

func checkSchema(s *SchemaConfig) error {
	if strings.TrimSpace(s.PathMatcher) == "" {
		return errors.New("patchMatcher must be non-empty")
	}
	_, err := syntax.Parse(s.PathMatcher, syntax.Perl)
	if err != nil {
		return fmt.Errorf("pathMatcher is invalid; %v", err)
	}
	if strings.TrimSpace(s.SourceId) == "" {
		return errors.New("sourceId must be non-empty")
	}
	return nil
}
