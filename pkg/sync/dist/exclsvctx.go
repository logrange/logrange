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
package dist

import (
	"context"

	"github.com/logrange/logrange/pkg/kv"
)

// AcquireExclusiveCtx allows to acquire the exclusive context by a name provided.
// The exclusive context cannot be acquired by any other go-routine of the
// process or any other participating process, until the previously
// acquired context is not closed or cancelled.
//
// The following rules are applied:
// - Only one go-routine in the distributed system can acquire the exclusive
//   context in a raise.
// - No go-routine can acquire the context, until there is another active
//   (not closed) one is in the distributed system
// - The returned context can be rearely closed by a reason, like a
// 	 system disaster. In the distributed system the context can be closed
// 	 by a network fault or partitioning, indicating that the context is not
// 	 valid and the consistency constrains cannot be supported due to
// 	 the malfunction. Implementation code may support the fault detection
// 	 functionality and close the context automatically.
// - If the context is found to be closed, it means that it is not safe
// 	 to perform operations that supposed to be guarded by the context and
// 	 the context must be req-acquired again.
//
// Example:
// 		cctx, cancel, err := sp.AcquireExclusiveCtx(context.TODO(), distStorage, "mylock")
// 		if err != nil {
// 			// handle the error
//			return
// 		}
// 		...
// 		if cctx.Err() != nil {
//			// a disaster happened and the context was lost,
//			return
//		}
//		...
//		// all the job was done, so we can close (release "mylock") the context
//		cancel()
func AcquireExclusiveCtx(ctx context.Context, st kv.Storage, name string) (context.Context, context.CancelFunc, error) {
	return nil, nil, nil
}
