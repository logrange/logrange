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

package main

import (
	"context"
	"fmt"
	"github.com/logrange/logrange/api"
	"github.com/logrange/logrange/api/rpc"
	"github.com/logrange/range/pkg/transport"
	"time"
)

func main() {
	// Creating rpc.Client using standard transport settings
	client, err := rpc.NewClient(transport.Config{
		ListenAddr: "127.0.0.1:9966",
	})

	if err != nil {
		fmt.Println("could not create client err=", err)
		return
	}
	defer client.Close()

	// The select below will read data from the partition with tag "partition=testWrite" until it is interrupted or an error
	// happens
	cnt := 0
	err = api.Select(context.Background(), client, &api.QueryRequest{Query: "SELECT FROM partition=testWrite", Limit: 100, WaitTimeout: 10}, true,
		func(qr *api.QueryResult) {
			for _, ev := range qr.Events {
				ts := time.Unix(0, ev.Timestamp)
				fmt.Println(cnt, ": ts=", ts, ", msg=", ev.Message)
				cnt++
			}
		},
	)
	fmt.Println("api.Select() returned the error=", err)
}
