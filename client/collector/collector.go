// Copyright 2018-2019 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/scanner"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/range/pkg/utils/bytes"
	"math/rand"
	"sync"
	"time"
)

func Run(ctx context.Context, cfg *scanner.Config, cl api.Client, storg storage.Storage) error {
	logger := log4g.GetLogger("collector")

	//return runTestRead(ctx, cl)
	return runTest(ctx, cl, 6000000)

	scanr, err := scanner.NewScanner(cfg, storg)
	if err != nil {
		return fmt.Errorf("failed to create scanner, err=%v", err)
	}

	events := make(chan *model.Event)
	if err := scanr.Run(ctx, events); err != nil {
		return fmt.Errorf("failed to run scanner, err=%v", err)
	}

	var wr api.WriteResult
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			break
		case ev := <-events:
			for ctx.Err() == nil {
				tgs, flds := toTagsAndFields(ev)
				err = cl.Write(ctx, tgs, flds, toApiEvents(ev), &wr)
				if err != nil {
					logger.Info("Communication error, retry in ", 5, "sec; cause: ", err)
					utils.Sleep(ctx, 5*time.Second)
					continue
				}

				if wr.Err != nil {
					logger.Warn("Error ingesting event=", ev, ", server err=", wr.Err)
					utils.Sleep(ctx, 5*time.Second)
					continue
				}

				ev.Confirm()
				break
			}
		}
	}

	_ = scanr.WaitAllJobsDone()
	close(events)

	logger.Info("Shutdown.")
	return err
}

func toTagsAndFields(ev *model.Event) (string, string) {
	ts := tag.MapToSet(ev.Meta.Tags)
	fs := tag.MapToSet(ev.Meta.Fields)
	return ts.Line().String(), fs.Line().String()
}

func runTest(ctx context.Context, cl api.Client, count int64) error {
	fmt.Println("starting test ")
	start := time.Now()

	var wg sync.WaitGroup
	threads := 4

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(tags string) {
			recs := makeTestEvents()
			var wr api.WriteResult
			for i := int64(0); i < count; i += int64(len(recs)) {
				err := cl.Write(ctx, tags, "", recs, &wr)
				if err != nil {
					fmt.Println("oops, got the error ", err)
					break
				}
				if i%500000 == 0 {
					fmt.Println(tags, " written ", i, " records")
				}
				updateTS(recs)
			}
			wg.Done()
		}(fmt.Sprintf("test=hello%d", i))
	}
	wg.Wait()

	lng := time.Now().Sub(start)

	gigs := int64(100) * int64(count) * int64(threads)
	gigsf := float64(gigs) / (float64(lng) / float64(time.Second))

	fmt.Println("count=", count, " records written for ", lng, " it is about ", lng/time.Duration(count), " speed is ", humanize.Bytes(uint64(gigsf)), "/s ", float64(int64(threads)*count)/float64(lng)/float64(time.Second), "recs/sec")
	return nil
}

func runTestRead(ctx context.Context, cl api.Client) error {
	fmt.Println("starting read test ")
	start := time.Now()

	var wg sync.WaitGroup
	threads := 1
	count := 2000000

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(tags string) {
			lc := count
			ctx1, cancel := context.WithCancel(ctx)
			api.Select(ctx1, cl, &api.QueryRequest{Query: "select from " + tags + " position head limit 1000", Limit: 1000}, true, func(res *api.QueryResult) {
				lc -= len(res.Events)
				if lc <= 0 {
					cancel()
				}
				if lc%100000 == 0 {
					fmt.Println("left ", lc, res.Events[0].Timestamp)
				}
			})
			wg.Done()
		}(fmt.Sprintf("test=hello%d", i))
	}
	wg.Wait()

	lng := time.Now().Sub(start)

	gigs := int64(100) * int64(count) * int64(threads)
	gigs = gigs / int64(lng/time.Second)

	fmt.Println("count=", count*threads, " records read for ", lng, " it is about ", lng/time.Duration(count), " speed is ", humanize.Bytes(uint64(gigs)), "/s ", int64(threads*count)/int64(lng/time.Second), "recs/sec")
	return nil
}

func updateTS(recs []*api.LogEvent) {
	ts := time.Now().UnixNano()
	for _, r := range recs {
		r.Timestamp = ts
		ts++
	}
}

func makeTestEvents() []*api.LogEvent {
	res := make([]*api.LogEvent, 0, 2000)
	var buf [100]byte
	for i := 0; i < cap(res); i++ {
		res = append(res, &api.LogEvent{
			Timestamp: time.Now().UnixNano(),
			Message:   bytes.ByteArrayToString(buf[:]),
		})
	}
	return res
}

func toApiEvents(ev *model.Event) []*api.LogEvent {
	res := make([]*api.LogEvent, 0, len(ev.Records))
	for _, r := range ev.Records {
		res = append(res, &api.LogEvent{
			Timestamp: r.Date.UnixNano(),
			Message:   bytes.ByteArrayToString(r.Data),
		})
	}
	return res
}
