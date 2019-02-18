// Copyright 2018 The logrange Authors
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
	"github.com/logrange/logrange/collector/model"
	"github.com/logrange/logrange/collector/remote"
	"github.com/logrange/logrange/collector/scanner"
	"github.com/logrange/logrange/collector/storage"
)

var (
	logger = log4g.GetLogger("collector")
)

func Run(cfg *Config, ctx context.Context) error {
	store, err := storage.NewStorage(cfg.Storage)
	if err != nil {
		return fmt.Errorf("failed to create storage, err=%v", err)
	}
	scanr, err := scanner.NewScanner(cfg.Scanner, store)
	if err != nil {
		return fmt.Errorf("failed to create scanner, err=%v", err)
	}
	client, err := remote.NewClient(cfg.Remote)
	if err != nil {
		return fmt.Errorf("failed to create client, err=%v", err)
	}

	logger.Info("Running...")
	events := make(chan *model.Event)

	err = client.Run(ctx, events)
	if err != nil {
		return fmt.Errorf("failed to run client, err=%v", err)
	}
	err = scanr.Run(events, ctx)
	if err != nil {
		return fmt.Errorf("failed to run scanner, err=%v", err)
	}

	<-ctx.Done()
	_ = scanr.Close()
	_ = client.Close()

	close(events)
	logger.Info("Shutdown.")
	return err
}
