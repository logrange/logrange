package collector

import (
	"encoding/json"
	"github.com/logrange/logrange/pkg/collector/ingestor"
	"github.com/logrange/logrange/pkg/collector/scanner"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

// Config struct just aggregate different types of configs in one place
type Config struct {
	Ingestor   *ingestor.Config `json:"ingestor"`
	Scanner    *scanner.Config  `json:"scanner"`
	StatusFile string           `json:"statusFile"`

	// StateFile contains a path to file where scanner will keep its scan states
	StateFile string `json:"stateFile"`
}

const (
	cDefaultConfigStatus = "/opt/logrange/collector/status"
	cDefaultStateFile    = "/opt/logrange/collector/state"
)

func NewDefaultConfig() *Config {
	return &Config{
		Ingestor:   ingestor.NewDefaultConfig(),
		Scanner:    scanner.NewDefaultConfig(),
		StatusFile: cDefaultConfigStatus,
		StateFile:  cDefaultStateFile,
	}
}

// LoadFromFile loads the config from file name provided in path
func (ac *Config) LoadFromFile(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	toLower := strings.ToLower(path)
	if strings.HasSuffix(toLower, "yaml") {
		return yaml.Unmarshal(data, ac)
	}
	return json.Unmarshal(data, ac)
}

// Apply sets non-empty fields value from ac1 to ac
func (ac *Config) Apply(ac1 *Config) {
	if ac1 == nil {
		return
	}

	ac.Ingestor.Apply(ac1.Ingestor)
	ac.Scanner.Apply(ac1.Scanner)
	if ac1.StatusFile != "" {
		ac.StatusFile = ac1.StatusFile
	}
	if ac1.StateFile != "" {
		ac.StateFile = ac1.StateFile
	}
}
