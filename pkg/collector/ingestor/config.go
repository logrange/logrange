package ingestor

import (
	"github.com/mohae/deepcopy"
)

type (
	// Config struct bears the ingestor configuration information. The
	// ingestor will use it for sending data to the aggragator. The structure
	// is used for configuring the ingestor
	Config struct {
		Server           string          `json:"server"`
		RetrySec         int             `json:"retrySec"`
		HeartBeatMs      int             `json:"heartBeatMs"`
		PacketMaxRecords int             `json:"packetMaxRecords"`
		AccessKey        string          `json:"accessKey"`
		SecretKey        string          `json:"secretKey"`
		Schemas          []*SchemaConfig `json:"schemas"`
	}

	// SchemaConfig struct contains information by matching a file (PathMatcher)
	// to the journal id (SourceId) it also contains tags that will be used
	// for the match.
	SchemaConfig struct {
		PathMatcher string            `json:"pathMatcher"`
		SourceId    string            `json:"sourceId"`
		Tags        map[string]string `json:"tags"`
	}
)

func NewDefaultConfig() *Config {
	ic := new(Config)
	ic.Server = "127.0.0.1:9966"
	ic.RetrySec = 5
	ic.PacketMaxRecords = 1000
	ic.HeartBeatMs = 15000
	ic.AccessKey = ""
	ic.SecretKey = ""
	ic.Schemas = NewDefaultSchemaConfigs()
	return ic
}

func NewDefaultSchemaConfigs() []*SchemaConfig {
	return []*SchemaConfig{
		{
			PathMatcher: "/*(?:.+/)*(?P<file>.+\\..+)",
			SourceId:    "{file}",
			Tags:        map[string]string{"file": "{file}"},
		},
	}
}

// Apply sets non-empty fields value from ic1 to ic
func (ic *Config) Apply(ic1 *Config) {
	if ic1 == nil {
		return
	}

	if ic1.Server != "" {
		ic.Server = ic1.Server
	}

	if ic1.RetrySec != 0 {
		ic.RetrySec = ic1.RetrySec
	}

	if ic1.PacketMaxRecords != 0 {
		ic.PacketMaxRecords = ic1.PacketMaxRecords
	}

	if ic1.HeartBeatMs != 0 {
		ic.HeartBeatMs = ic1.HeartBeatMs
	}

	if ic1.AccessKey != "" {
		ic.AccessKey = ic1.AccessKey
	}

	if ic1.SecretKey != "" {
		ic.SecretKey = ic1.SecretKey
	}

	if len(ic1.Schemas) != 0 {
		ic.Schemas = deepcopy.Copy(ic1.Schemas).([]*SchemaConfig)
	}
}

