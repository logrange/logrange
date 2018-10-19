package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/logrange/logrange/pkg/collector"
	"github.com/logrange/logrange/pkg/collector/ingestor"
	"github.com/logrange/logrange/pkg/collector/scanner"
	"os"
	"os/signal"
	"syscall"

	"github.com/ghodss/yaml"
	"github.com/jrivets/log4g"
	"gopkg.in/alecthomas/kingpin.v2"
)

type (
	args struct {
		config      string
		debug       bool
		printStatus bool
		printConfig string
	}
)

const (
	Version            = "0.0.1"
	cDefaultConfigPath = "/opt/logrange/collector/config.json"
)

var (
	logger = log4g.GetLogger("collector")
)

func main() {
	defer log4g.Shutdown()
	args := parseCLP()
	if args.debug {
		log4g.SetLogLevel("", log4g.TRACE)
	}

	cfg := collector.NewDefaultConfig()
	err := cfg.LoadFromFile(args.config)
	logger.Info("Loading config from ", args.config)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Error("Unable to load config file=", args.config, "; cause err=", err)
			os.Exit(1)
		}

		logger.Warn("Unable to load config file=", args.config, "; The file is not found")
		cfg = collector.NewDefaultConfig()
	}

	if args.printConfig != "" {
		var bCfg []byte
		switch args.printConfig {
		case "json":
			bCfg, err = json.MarshalIndent(cfg, "", "    ")
		case "yaml":
			bCfg, err = yaml.Marshal(cfg)
		default:
			logger.Error("Unsupported config format ", args.printConfig, " 'json' or 'yaml' can be used.")
			os.Exit(1)
		}

		if err != nil {
			logger.Error("Internal error, could not form config in YAML format: ", err)
			os.Exit(1)
		}
		fmt.Println("")
		fmt.Println(string(bCfg))
		os.Exit(0)
	}

	if args.printStatus {
		su := collector.NewStatusFileUpdater(cfg, nil, nil)
		su.PrintStatusFile()
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-sigChan:
			logger.Warn("Handling signal=", s)
			cancel()
		}
	}()

	var (
		scn *scanner.Scanner
		ing *ingestor.Ingestor
	)

	scn, err = newScanner(cfg)
	if err != nil {
		logger.Fatal("Unable to create scanner; cause: ", err)
		return
	}
	defer scn.Stop()

	ing, err = ingestor.NewIngestor(cfg.Ingestor, ctx)
	if err != nil {
		logger.Fatal("Unable to create ingestor; cause: ", err)
		return
	}

	sfu := collector.NewStatusFileUpdater(cfg, scn, ing)
	sfu.Run(ctx)

	done := ing.Run(ctx, scn.Events())
	<-done
}

func parseCLP() *args {
	var (
		config      = kingpin.Flag("config-file", "Configuration file name").Default(cDefaultConfigPath).String()
		debug       = kingpin.Flag("debug", "Enable debug log level").Bool()
		printConfig = kingpin.Flag("print-config", "Print existing config").PlaceHolder("(json|yaml)").String()
		status      = kingpin.Flag("print-status", "Print agent status, if it is running").Bool()
	)
	kingpin.Version(Version)
	kingpin.Parse()

	res := new(args)
	res.config = *config
	res.debug = *debug
	res.printStatus = *status
	res.printConfig = *printConfig
	return res
}

func newScanner(cfg *collector.Config) (*scanner.Scanner, error) {
	gsr, err := scanner.NewScanner(cfg.Scanner, scanner.NewFileStateStorage(cfg.StateFile))
	if err == nil {
		err = gsr.Start()
	}
	return gsr, err
}
