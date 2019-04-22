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

package rpc

import (
	"context"
	"github.com/logrange/range/pkg/utils/encoding/xbinary"
	"github.com/pkg/errors"
	"io"
	"sync"
	"sync/atomic"
)

type (
	// Client allows to make remote calls to the server
	Client interface {
		io.Closer
		BufCollector

		// Call makes a call to the server. Expects function Id, the message, which has to be sent. The Call will block the
		// calling go-routine until ctx is done, a response is received or an error happens related to the Call processing.
		// It returns response body, opErr contains the function execution error, which doesn't relate to the connection.
		// The err contains an error related to the call execution (connection problems etc.)
		Call(ctx context.Context, funcId int, msg xbinary.Writable) (respBody []byte, opErr error, err error)
	}

	// OnClientReqFunc represents a server endpoint for handling a client call. The function will be selected by Server
	// by the funcId received in the request.
	OnClientReqFunc func(reqId int32, reqBody []byte, sc *ServerConn)

	// Server supports server implementation for client requests handling.
	Server interface {
		io.Closer
		// Register adds a callback which will be called by the funcId
		Register(funcId int, cb OnClientReqFunc) error
		// Serve is called for sc - the server code which will be served until the codec is closed or the server is
		// shutdown
		Serve(connId string, rwc io.ReadWriteCloser) error
	}

	// BufCollector allows to collect byte buffers that are not going to be used anymore. The buffers could be
	// reused by Client or the Server later for processing requests and responses. Must be used with extra care
	BufCollector interface {
		// Collect stores the buf into the pool, to be reused for future needs
		Collect(buf []byte)
	}
)

var knwnErrors atomic.Value
var errsMtx sync.Mutex

// RegisterError allows to register an error like io.EOF, which then will be returned by its name from Client.Call as
// opError
func RegisterError(err error) {
	errsMtx.Lock()
	defer errsMtx.Unlock()

	newMap := make(map[string]error)
	knwnMap, _ := knwnErrors.Load().(map[string]error)
	for k, v := range knwnMap {
		newMap[k] = v
	}
	newMap[err.Error()] = err
	knwnErrors.Store(newMap)
}

func errorByText(errTxt string) error {
	if mp, ok := knwnErrors.Load().(map[string]error); ok {
		if err, ok := mp[errTxt]; ok {
			return err
		}
	}
	return errors.New(errTxt)
}
