package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/logrange/logrange/pkg/collector/ingestor"
	"github.com/logrange/logrange/pkg/collector/scanner"
	"github.com/logrange/logrange/pkg/util"
	"io/ioutil"
	"math"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jrivets/log4g"
)

type (
	// StatusFileUpdater struct allows to collect information from scanner, ingestor, and config.
	StatusFileUpdater struct {
		cfg    *Config
		scn    *scanner.Scanner
		ing    *ingestor.Ingestor
		logger log4g.Logger
	}
)

const (
	cStatFileUpdateSec = 5 * time.Second
)

func NewStatusFileUpdater(cfg *Config, scn *scanner.Scanner, ing *ingestor.Ingestor) *StatusFileUpdater {
	sfu := new(StatusFileUpdater)
	sfu.cfg = cfg
	sfu.scn = scn
	sfu.ing = ing
	sfu.logger = log4g.GetLogger("collector.ingestor.statusFileUpdater")
	return sfu
}

func (sfu *StatusFileUpdater) PrintStatusFile() {
	res, err := ioutil.ReadFile(sfu.cfg.StatusFile)
	if err != nil {
		fmt.Println("ERROR: Seems no agent running.")
		return
	}
	fmt.Println(string(res))
}

func (sfu *StatusFileUpdater) Run(ctx context.Context) {
	if sfu.cfg.StatusFile == "" {
		sfu.logger.Warn("Could not run status file update, file is not set up.")
		return
	}

	go func() {
		sfu.logger.Info("Will update status every ", cStatFileUpdateSec, " to ", sfu.cfg.StatusFile)
		defer os.Remove(sfu.cfg.StatusFile)
		for {
			sfu.saveStatFile()
			select {
			case <-ctx.Done():
				sfu.logger.Info("Stop writing stat file")
				return
			case <-time.After(cStatFileUpdateSec):
			}
		}
	}()
}

func (sfu *StatusFileUpdater) saveStatFile() {
	var w bytes.Buffer
	tw := new(tabwriter.Writer)
	tw.Init(&w, 0, 8, 1, ' ', 0)

	fmt.Fprintf(tw, "*********************\n")
	fmt.Fprintf(tw, "* Logrange Collector \n")
	fmt.Fprintf(tw, "*********************\n")
	fmt.Fprintf(tw, "\n=== Configuration file ===\n")
	buf, _ := json.MarshalIndent(sfu.cfg, "", "   ")
	fmt.Fprintf(tw, string(buf))
	fmt.Fprintf(tw, "\n\nStatus at %s\n\n", time.Now().String())
	fmt.Fprintf(tw, "=== Connection ===\n")
	fmt.Fprintf(tw, "\tAggregator:\t%s\n", sfu.cfg.Ingestor.Server)
	fmt.Fprintf(tw, "\tRecs per packet:\t%d\n", sfu.cfg.Ingestor.PacketMaxRecords)
	fmt.Fprintf(tw, "\tHeartbeat:\t%dms\n", sfu.cfg.Ingestor.HeartBeatMs)

	if sfu.ing.IsConnected() {
		fmt.Fprintf(tw, "\tStatus:\tCONNECTED\n")
	} else {
		fmt.Fprintf(tw, "\tStatus:\tCONNECTING...\n")
	}

	fmt.Fprintf(tw, "\n=== Scanner ===\n")

	gs := sfu.scn.GetStats()
	fmt.Fprintf(tw, "\tscan paths:\t%v\n", gs.Config.ScanPaths)
	fmt.Fprintf(tw, "\tscan intervals:\tevery %d sec.\n", gs.Config.ScanPathsIntervalSec)
	fmt.Fprintf(tw, "\tstate file:\t%s\n", sfu.cfg.StateFile)
	fmt.Fprintf(tw, "\tstate update:\tevery %d sec.\n", gs.Config.StateFlushIntervalSec)
	fmt.Fprintf(tw, "\tfile formats:\t%v\n", gs.Config.FileFormats)
	fmt.Fprintf(tw, "\trecord max size:\t%s\n", util.FormatSize(int64(gs.Config.RecordMaxSizeBytes)))
	fmt.Fprintf(tw, "\trecords per pack:\t%d\n\n", gs.Config.EventMaxRecords)
	fmt.Fprintf(tw, "--- scanned files (%d) ---\n", len(gs.Workers))
	for _, wkr := range gs.Workers {
		fmt.Fprintf(tw, "\t%s\n", wkr.Filename)
	}

	fmt.Fprintf(tw, "\n--- excluded files (%d) ---\n", len(gs.Excludes))
	for _, ef := range gs.Excludes {
		fmt.Fprintf(tw, "\t%s\n", ef)
	}

	knwnTags := sfu.ing.GetKnownTags()
	totalPerc := float64(0)
	for i, wkr := range gs.Workers {
		fmt.Fprintf(tw, "\n--- Scanner %d\n", i+1)
		fmt.Fprintf(tw, "\tfile:\t%s\n", wkr.Filename)
		fmt.Fprintf(tw, "\tdesc-id:\t%s\n", wkr.Id)
		fmt.Fprintf(tw, "\tdata-type:\t%s\n", wkr.ParserStats.DataType)
		size := wkr.ParserStats.Size
		fmt.Fprintf(tw, "\tsize:\t%s\n", util.FormatSize(size))

		pos := wkr.ParserStats.Pos
		perc := float64(100)
		if size > 0 {
			perc = float64(pos) * perc / float64(size)
		}
		perc = math.Max(0.0, math.Min(100.0, perc))
		totalPerc += perc

		if tags, ok := knwnTags[wkr.Filename]; ok {
			fmt.Fprintf(tw, "\tknwnTags: \n\t%s", tags)
		} else {
			fmt.Fprintf(tw, "\tknwnTags:\t<data is not sent yet, or no new data for 5 mins>\n")
		}

		fmt.Fprintf(tw, "\tprogress:\t%s %s\n", util.FormatSize(pos), util.FormatProgress(30, perc))
		if len(wkr.ParserStats.DateFormats) > 0 {
			fmt.Fprintf(tw, "\n\tFormats:\n")
			tot := int64(0)
			for _, v := range wkr.ParserStats.DateFormats {
				tot += v
			}
			for dtf, v := range wkr.ParserStats.DateFormats {
				perc := float64(v) * 100.0 / float64(tot)
				fmt.Fprintf(tw, "\t\t\"%s\"\t%5.2f%%(%d of %d records have the format)\n", dtf, perc, v, tot)
			}
		}
		fmt.Fprintf(tw, "-----------\n")
	}
	if len(gs.Workers) > 0 {
		totalPerc /= float64(len(gs.Workers))
	}
	fmt.Fprintf(tw, "\nReplica status: %s\n", util.FormatProgress(40, totalPerc))

	tw.Flush()
	ioutil.WriteFile(sfu.cfg.StatusFile, []byte(w.Bytes()), 0644)
}
