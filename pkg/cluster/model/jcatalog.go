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

package model

import (
	"context"
	"encoding/json"

	"github.com/jrivets/log4g"
	"github.com/logrange/logrange/pkg/cluster"
	"github.com/logrange/logrange/pkg/kv"
	"github.com/logrange/logrange/pkg/records/chunk"
	"github.com/logrange/logrange/pkg/util"
	"github.com/pkg/errors"
)

type (
	// JournalCatalog allows to access to the journals information known in the cluster
	// The information reported by every host will be associated with the process
	// Lease (HostRegistry.Lease()) and it will be removed from the storage as soon,
	// as the process is down.
	//
	// Every journal is represented by multiple records in the kv.Storage by
	// the following key schema:
	//
	//    J/<journal>/<Host>
	//
	// So a journal is a set of records. When the host is down, it's records
	// will be disappeared due to the record's lease expiration
	JournalCatalog interface {
		// GetJournalInfo returns information known for a journal by its name
		GetJournalInfo(ctx context.Context, jname string) (JournalInfo, error)

		// ReportLocalChunks reports information about local chunks.
		ReportLocalChunks(ctx context.Context, jname string, chunks []chunk.Id) error
	}

	// journalCatalog implements JournalCatalog interface
	journalCatalog struct {
		Strg   kv.Storage   `inject:""`
		HstReg HostRegistry `inject:""`

		logger log4g.Logger
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

func NewJournalCatalog() *journalCatalog {
	jc := new(journalCatalog)
	jc.logger = log4g.GetLogger("cluster.model.JournalCatalog")
	return jc
}

// GetJournalInfo returns information known for a journal by its name
func (jc *journalCatalog) GetJournalInfo(ctx context.Context, jname string) (JournalInfo, error) {
	jc.logger.Trace("GetJournalInfo(), jname=", jname)

	startKey := kv.Key(jc.getJournalKey(jname, false))
	recs, err := jc.Strg.GetRange(ctx, startKey, kv.Key(jc.getJournalKey(jname, true)))
	if err != nil {
		return JournalInfo{}, errors.Wrap(err, "Could not receive Journal records")
	}

	var res JournalInfo
	res.Journal = jname
	res.LocalChunks = make([]chunk.Id, 0, 10)
	res.Chunks = make(map[chunk.Id]ChunkHosts)

	for _, r := range recs {
		sid := getKeySuffix(r.Key, string(startKey))
		hid, err := cluster.ParseHostId(sid)
		if err != nil {
			jc.logger.Error("GetJournalInfo(): Read the key=", r.Key, " and could not parse suffix ", sid, " as its HostId. Skipping the record. err=", err)
			continue
		}

		var cids []chunk.Id
		err = json.Unmarshal(r.Value, &cids)
		if err != nil {
			jc.logger.Error("GetJournalInfo(): Could not unmarshal value=", string(r.Value), " to []chunk.Id. hid=", hid, ". Skipping it. err=", err)
			continue
		}

		if hid == jc.HstReg.Id() {
			res.LocalChunks = cids
			continue
		}

		for _, cid := range cids {
			chsts, ok := res.Chunks[cid]
			if !ok {
				chsts = make(ChunkHosts, 0, 1)
			}
			chsts = append(chsts, hid)
			res.Chunks[cid] = chsts
		}
	}

	return res, nil

}

// ReportLocalChunks reports information for the journal.
func (jc *journalCatalog) ReportLocalChunks(ctx context.Context, jname string, chunks []chunk.Id) error {
	key := kv.Key(jc.getJournalKey(jname, false) + jc.HstReg.Id().String())
	jc.logger.Debug("ReportLocalChunks(), jname=", jname, " chunks=", chunks, ", using key=", key)

	if len(chunks) == 0 {
		jc.logger.Info("ReportLocalChunks(): the chunks list is empty for jname=", jname, ", removing key=", key, " from the storage")

		err := jc.Strg.Delete(ctx, key)
		if err == nil || err == kv.ErrNotFound {
			return nil
		}

		return errors.Wrapf(err, "Could not delete key=%s for journal %s", key, jname)
	}

	newVal, err := json.Marshal(chunks)
	if err != nil {
		return errors.Wrapf(err, "Could not Marshal chunks for jname=%s chunks=%v", jname, chunks)
	}

	for {
		rec, err := jc.Strg.Get(ctx, key)

		if err == kv.ErrNotFound {
			rec.Key = key
			rec.Value = newVal
			rec.Lease = jc.HstReg.Lease().Id()
			_, err := jc.Strg.Create(ctx, rec)
			if err == nil {
				jc.logger.Debug("ReportLocalChunks(): successfully created chunks for jname=", jname)
				return nil
			}

			if err == kv.ErrAlreadyExists {
				// oops, raise
				jc.logger.Warn("ReportLocalChunks(): raise coniditon while creating the chunks for jname=", jname, ", looping")
				continue
			}

			return errors.Wrapf(err, "ReportLocalChunks(): unexpected error while creating chunks fo jname=%s, key=%s", jname, key)
		}

		if err != nil {
			return errors.Wrapf(err, "Could not read data for jname=%s key=%s", jname, key)
		}

		// ok, we got the key value, it seems like we have to update it then
		rec.Value = newVal
		_, err = jc.Strg.CasByVersion(ctx, rec)
		if err == nil {
			return nil
		}

		if err != kv.ErrWrongVersion && err != kv.ErrNotFound {
			return errors.Wrapf(err, "ReportLocalChunks(): unexpected error while updating chunks for jname=%s, key=%s", jname, key)
		}
		jc.logger.Warn("ReportLocalChunks(): raise coniditon while updating the chunks for jname=", jname, ", looping")
	}
}

func (jc *journalCatalog) getJournalKey(jname string, last bool) string {
	jk := keyJournalPfx + util.EscapeToFileName(jname)
	if last {
		return jk + "0"
	}
	return jk + "/"
}
