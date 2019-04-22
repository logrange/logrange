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

package ctrlr

import (
	"context"
	"fmt"

	"github.com/logrange/range/pkg/records/chunk"
	"github.com/logrange/range/pkg/records/journal"
)

type (
	// chnksController provides journal.ChnkController implementation. It supports
	// local chunks only so far
	chnksController struct {
		name    string
		adv     *advertiser
		localCC *fsChnksController
		clstnr  *chunkListener
	}
)

func newChunksController(name string, localCC *fsChnksController, adv *advertiser) *chnksController {
	cc := new(chnksController)
	cc.name = name
	cc.localCC = localCC
	cc.adv = adv
	cc.clstnr = newChunkListener(cc)
	cc.localCC.ckListener = cc.clstnr
	return cc
}

func (cc *chnksController) String() string {
	return fmt.Sprintf("{name:%s, adv:%v, localCC:%v, clstnr:%v}", cc.name, cc.adv, cc.localCC, cc.clstnr)
}

func (cc *chnksController) ensureInit() {
	cc.localCC.ensureInit()
}

func (cc *chnksController) isEmpty() bool {
	return cc.localCC.isEmpty()
}

func (cc *chnksController) shutdown(ctx context.Context) {
	cc.localCC.close()
	cc.clstnr.close()
}

// JournalName is part of journal.ChnksController interface
func (cc *chnksController) JournalName() string {
	return cc.name
}

// GetChunkForWrite is part of journal.ChnksController interface
func (cc *chnksController) GetChunkForWrite(ctx context.Context, excludeCid chunk.Id) (chunk.Chunk, error) {
	ck, newCk, err := cc.localCC.getChunkForWrite(ctx, excludeCid)
	if newCk {
		cc.adv.advertise(cc.name, cc.localCC.getAdvChunks())
	}
	return ck, err
}

// Chunks is part of journal.ChnksController interface
func (cc *chnksController) Chunks(ctx context.Context) (chunk.Chunks, error) {
	return cc.localCC.getChunks(ctx)
}

// WaitForNewData is part of journal.ChnksController interface
func (cc *chnksController) WaitForNewData(ctx context.Context, pos journal.Pos) error {
	return cc.clstnr.waitData(ctx, pos)
}

// DeleteChunks marks journal's chunks with id <= lastCid as deleted
func (cc *chnksController) DeleteChunks(ctx context.Context, lastCid chunk.Id, cdf journal.OnChunkDeleteF) (int, error) {
	return cc.localCC.deleteChunks(ctx, lastCid, cdf)
}

func (cc *chnksController) LocalFolder() string {
	return cc.localCC.dir
}

func (cc *chnksController) getLastChunk(ctx context.Context) (chunk.Chunk, error) {
	return cc.localCC.getLastChunk(ctx)
}
