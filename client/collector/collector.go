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
	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/scanner"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/logrange/range/pkg/utils/bytes"
	"time"
)

func Run(ctx context.Context, cfg *scanner.Config, cl api.Client, storg storage.Storage) error {
	logger := log4g.GetLogger("collector")

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

func toApiEvents(ev *model.Event) []*api.LogEvent {
	res := make([]*api.LogEvent, 0, len(ev.Records))
	for _, r := range ev.Records {
		res = append(res, &api.LogEvent{
			Timestamp: r.Date.UnixNano(),
			Message:   bytes.ByteArrayToString(r.Data),
			Fields:    r.Fields,
		})
	}
	return res
}
