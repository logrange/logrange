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

	// preparing records
	recs := []*api.LogEvent{
		{
			Timestamp: time.Now().UnixNano(),
			Message:   "Hello Logrange, first message",
		}, {
			Timestamp: time.Now().UnixNano(),
			Message:   "Hello Logrange, second message",
		},
	}

	// Ingesting the recs into the partition with one tag "partition=testWrite", and apply
	// the fields values "custField=test" to all records from the recs
	var wr api.WriteResult
	err = client.Write(context.Background(), "partition=testWrite", "custField=test",
		recs, &wr)

	if err != nil {
		fmt.Println("could not send data to the server. err=", err)
		return
	}

	if wr.Err != nil {
		fmt.Println("server could not write the records and it returned the error err=", wr.Err)
		return
	}

	fmt.Println(len(recs), " records were successfully written. Try to see what is there now by 'SELECT FROM partition=testWrite'")

	// Read records from the table now
	var qr api.QueryResult
	err = client.Query(context.Background(), &api.QueryRequest{Query: "SELECT FROM partition=testWrite", Limit: 100}, &qr)
	if err != nil {
		fmt.Println("could not execute query request. err=", err)
		return
	}

	if qr.Err != nil {
		fmt.Println("server could not read the records and it returned the error err=", qr.Err)
		return
	}

	fmt.Println("Query result=", qr.String())
	for i, ev := range qr.Events {
		ts := time.Unix(0, ev.Timestamp)
		fmt.Println(i, ": ts=", ts, ", msg=", ev.Message)
	}
}
