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

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cluster/model"
	"github.com/logrange/logrange/pkg/records/chunk"
)

type (
	chnksController struct {
		name        string
		jrnlCatalog model.JournalCatalog
		adv         *advertiser
		localCC     *fsChnksController
		logger      log4g.Logger
		clsdCh      chan struct{}
	}
)

func newChunksController(name string, jrnlCatalog model.JournalCatalog, localCC *fsChnksController, adv *advertiser) *chnksController {
	cc := new(chnksController)
	cc.name = name
	cc.jrnlCatalog = jrnlCatalog
	cc.localCC = localCC
	cc.adv = adv
	cc.logger = log4g.GetLogger("chunksController").WithId("{" + name + "}").(log4g.Logger)
	cc.clsdCh = make(chan struct{})

	return cc
}

func (cc *chnksController) shutdown(ctx context.Context) {
	close(cc.clsdCh)
	go func() {
		cc.adv.advertise(cc.name, nil)
	}()
}

// JournalName is part of journal.ChnksController interface
func (cc *chnksController) JournalName() string {
	return cc.name
}

// GetChunkForWrite is part of journal.ChnksController interface
func (cc *chnksController) GetChunkForWrite(ctx context.Context) (chunk.Chunk, error) {
	ck, newCk, err := cc.localCC.getChunkForWrite(ctx)
	if newCk {
		cc.adv.advertise(cc.name, cc.localCC.getAdvChunks())
	}
	return ck, err
}

// Chunks is part of journal.ChnksController interface
func (cc *chnksController) Chunks(ctx context.Context) (chunk.Chunks, error) {
	return cc.localCC.getChunks(ctx)
}
