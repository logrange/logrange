package jrnlfs

import (
	"github.com/logrange/logrange/pkg/records/journal"
)

type (
	// Controller struct allows to scan provided folder on a file-system,
	// collects information about possible logrange database and manages
	// found journals via its interface
	//
	// The Controller implements journal.Controller and journal.ChunksController
	Controller struct {
	}
)

func NewController() *Controller {
	c := new(Controller)
	return c
}

// ----------------------- journal.Controller -------------------------------
func (c *Controller) GetOrCreate(ctx context.Context, jname string) (journal.Journal, error) {

}

// --------------------- journal.ChunksController ----------------------------
func (c *Controller) GetChunks(jid uint64) chunk.Chunks {

}

func (c *Controller) NewChunk(ctx context.Context, jid uint64) (chunk.Chunk, error) {

}
