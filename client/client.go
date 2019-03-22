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

package client

import (
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/api/rpc"
	"github.com/logrange/logrange/pkg/storage"
	"github.com/logrange/range/pkg/transport"
)

func NewClient(cfg transport.Config) (api.Client, error) {
	cli, err := rpc.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create client, err=%v", err)
	}
	return cli, err
}

func NewStorage(cfg *storage.Config) (storage.Storage, error) {
	strg, err := storage.NewStorage(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage, err=%v", err)
	}
	return strg, err
}
