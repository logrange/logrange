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

package data

import (
	"context"

	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	// JournalCatalog allows to access to the journals information known in the cluster
	JournalCatalog interface {
		// GetJournalInfo returns information known for a journal by its name
		GetJournalInfo(ctx context.Context, jname string) (JournalInfo, error)

		// ReportLocalChunks reports information for the journal. The information reported
		// by the function will be associated with the process Lease (HostRegistry.Lease())
		// and will be removed from the storage as soon, as the process is down.
		ReportLocalChunks(jname string, chunks []chunk.Id) error
	}

	ChunkHosts []cluster.HostId

	// JournalInfo contains information about a journal, collected by all hosts
	JournalInfo struct {
		// Journal is the name of the journal for which the information is collected
		Journal string

		// LocalChunks contains list of chunks available (reported) from the host
		LocalChunks []chunk.Id

		// Chunks contains map of chunks that are reported by other hosts for the journal
		Chunks map[chunk.Id]ChunkHosts
	}
)
