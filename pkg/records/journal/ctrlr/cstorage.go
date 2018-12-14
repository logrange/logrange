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

package ctrlr

import (
	"context"

	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	// ChunkStorage interface allows to have an access to a chunk storage.
	// Chunks could be stored localy (on local FS) or remotely. The interface
	// unifys access to different types of chunks.
	ChunkStorage interface {
		// Scan returns slice of sorted chunk Ids found in the storage
		Scan(ctx context.Context) ([]chunk.Id, error)

		// Chunks returns all known chunks ordered by their Ids
		Chunks(ctx context.Context) ([]chunk.Chunk, error)

		GetChunkForWrite(ctx context.Context) (chunk.Chunk, error)
	}

	// ChunkStorageLocator defines address of the ChunkStorage
	ChunkStorageLocator struct {
		// Type defines the storage type
		Type int

		// Address contains an identifier of the storage (depends on its type)
		Address string
	}

	// ChunkStorageProvider interface provides access to chunk storage by its locator
	ChunkStorageProvider interface {
		GetChunkStorage(ctx context.Context, csLocator ChunkStorageLocator, jname string) (ChunkStorage, error)
	}
)

var CSLocalLocator = ChunkStorageLocator{CSTypeLocal, ""}

const (
	// CSTypeLocal - local chunk storage on the local file-system
	CSTypeLocal = iota

	// CSTypeLogrange chunk storage on another logrange instance (remote)
	CSTypeLogrange
)
