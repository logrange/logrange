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
	"github.com/logrange/logrange/client"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/logrange/pkg/scanner"
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/logrange/pkg/utils"
	"time"
	"unsafe"
)

func Run(ctx context.Context, cfg *client.Config,
	cl api.Client, storg storage.Storage) error {

	logger := log4g.GetLogger("collector")
	scanr, err := scanner.NewScanner(cfg.Collector, storg)
	if err != nil {
		return fmt.Errorf("failed to create scanner, err=%v", err)
	}

	logger.Info("Running...")
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
				tl := toTagLine(ev)
				// TODO: writing the tags and fields are all together. Must be changed when ready
				err = cl.Write(ctx, tl, tl, toApiEvents(ev), &wr)
				if err != nil {
					logger.Info("Communication error, retry in ", 5, "sec; cause: ", err)
					utils.Sleep(ctx, 5*time.Second)
				}
				if err == nil {
					if wr.Err != nil {
						logger.Warn("Error ingesting event=", ev, ", server err=", wr.Err)
					}
					ev.Confirm()
					break
				}
			}
		}
	}

	_ = scanr.Close()
	close(events)

	logger.Info("Shutdown.")
	return err
}

func toTagLine(ev *model.Event) string {
	set := tag.MapToSet(ev.Meta.Tags)
	return string(set.Line())
}

func toApiEvents(ev *model.Event) []*api.LogEvent {
	res := make([]*api.LogEvent, 0, len(ev.Records))
	for _, r := range ev.Records {
		res = append(res, &api.LogEvent{
			Timestamp: uint64(r.Date.UnixNano()),
			Message:   *(*string)(unsafe.Pointer(&r.Data)),
		})
	}
	return res
}
