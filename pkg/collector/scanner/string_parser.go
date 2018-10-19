package scanner

import (
	"fmt"
	"github.com/logrange/logrange/pkg/collector/model"
	"github.com/logrange/logrange/pkg/collector/scanner/dtparser"
	"io"
	"time"
)

type (
	// string_parser provides every non-empty string, so it  returns Record for
	// every input string
	stringParser struct {
		parser    *dtparser.Parser
		lastTS    time.Time
		fmtParser *dtparser.ParserFormat
		stats     *linePStat

		// maxSkip defines maximum value of the lines must be skipped.
		// if it is -1, then any line should not be attempted to find a format,
		// but current timestamp will be used
		maxSkip int
		skipped int
		state   int
	}

	// line parser statistics
	linePStat struct {
		hits map[string]int64
	}
)

const (
	slpStateNoFormat = iota
	slpStateAdjusting
	slpStateSkipping
)

// new_string_parser creates new string_parser, expects custom
// dtFormats in form "MMM D, YYYY h:mm:ss P" (see dtparser package) etc. if any.
func newStringParser(dtFormats ...string) *stringParser {
	slp := new(stringParser)

	if len(dtFormats) > 0 {
		dtFmts := make([]string, len(dtparser.KnownFormats)+len(dtFormats))
		copy(dtFmts[:len(dtFormats)], dtFormats)
		copy(dtFmts[len(dtFormats):], dtparser.KnownFormats)
		slp.parser = dtparser.NewParser(dtFmts)
	} else {
		slp.parser = dtparser.NewDefaultParser()
	}

	slp.stats = newLinePStat()
	slp.maxSkip = 10
	slp.state = slpStateAdjusting
	return slp
}

func (slp *stringParser) apply(line []byte) *model.Record {
	rec := model.NewEmptyRecord()
	rec.Data = line
	slp.applyTimestamp(line, rec)
	return rec
}

func (slp *stringParser) applyTimestamp(line []byte, rec *model.Record) {
	var (
		err error
		tm  time.Time
	)

	fp := slp.fmtParser
	switch slp.state {
	case slpStateNoFormat:
		slp.lastTS = time.Now()
	case slpStateAdjusting:
		if fp != nil {
			tm, err = fp.Parse(line)
			if err == nil {
				slp.lastTS = tm
				break
			}
		}

		tm, fp = slp.parser.Parse(line, fp)
		if fp != nil {
			slp.maxSkip = 1
			slp.skipped = 0
			slp.fmtParser = fp
			slp.lastTS = tm
			break
		}

		slp.fmtParser = nil
		slp.skipped++
		if slp.skipped > slp.maxSkip {
			slp.state = slpStateSkipping
			slp.skipped = 0
		}

	case slpStateSkipping:
		slp.skipped++
		if slp.skipped > slp.maxSkip {
			slp.state = slpStateAdjusting
			if slp.maxSkip < 128 {
				slp.maxSkip <<= 1
			}
			slp.skipped = slp.maxSkip
		}
	}

	slp.hitStats(fp)
	rec.SetTs(slp.lastTS)
}

func (slp *stringParser) hitStats(pf *dtparser.ParserFormat) {
	if slp.stats == nil {
		return
	}
	slp.stats.hit(pf)
}

func newLinePStat() *linePStat {
	lps := new(linePStat)
	lps.hits = make(map[string]int64)
	return lps
}

func (lps *linePStat) hit(pf *dtparser.ParserFormat) {
	name := "unknown"
	if pf != nil {
		name = pf.GetFormat()
	}
	lps.hits[name]++
}

func (lps *linePStat) PrintStatus(w io.Writer) {
	total := int64(0)
	for _, v := range lps.hits {
		total += v
	}

	fmt.Fprintf(w, "--- Found formats\n")
	for k, v := range lps.hits {
		perc := float32(100*v) / float32(total)
		fmt.Fprintf(w, "\"%s\"\t%d hit(s)(%5.2f%%) \n", k, v, perc)
	}

	unknw := lps.hits["unknown"]
	undidx := float32(100.0)
	if total > 0 {
		undidx = 100.0 - (float32(unknw) * 100.0 / float32(total))
	}

	fmt.Fprintf(w, "total: %d records consiered, successIdx %5.2f%%\n",
		total, undidx)
}
