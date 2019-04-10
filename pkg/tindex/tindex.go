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

package tindex

import (
	"github.com/logrange/logrange/pkg/lql"
	"github.com/logrange/logrange/pkg/model/tag"
)

type (
	// Service interface provides an access to the Tags index. It is used for selecting
	// a partition sources by the tag lines provided and by selecting journals by an expression.
	Service interface {
		// GetOrCreateJournal returns the partition name for the unique Tags combination. If the result
		// is returned with no error, the JournalName MUST be released using the Release method later
		GetOrCreateJournal(tags string) (string, tag.Set, error)

		// Visit walks over the tags-sources that corresponds to the srcCond. VF_SKIP_IF_LOCKED allows to skip the source if it
		// is locked. If VF_SKIP_IF_LOCKED is not set, the Visit will wait until the source become available or removed.
		// VF_DO_NOT_RELEASE will not release the partition automatically, but it is the client responsibility to release
		// the partition later
		Visit(srcCond *lql.Source, v VisitorF, visitFlags int) error

		// LockExclusively changes the acquired source jn (via GetOrCreateJournal or Visit) to exclusive lock.
		// If it returns true, the partition will not be returned in Visit and GetOrCreateJournal will be blocked unitl
		// it is released. The partition must be un-locked the exclusivelies, it was not deleted.
		LockExclusively(jn string) bool

		// UnlockExclusively removes the exclusive lock from the partition. If the lock is not acquired, will panic
		UnlockExclusively(jn string)

		// Release allows to release the partition name which could be acquired by GetOrCreateJournal
		Release(jn string)

		// Delete allows to delete a partition. It must be exclusively locked before the call. If the partition
		// was deleted, the consequireve Release() call will not have any effect. If the Delete returns any
		// error, the partition must be unlocked and released if it was acquired before
		Delete(jn string) error
	}

	// VisitorF is the callback function which si called by Service.Visit for all matches found. It will iterate
	// over the visit set until it is over or the function returns false. While the function is called the partition
	// name will be hold as acquired, so delete will not work at the moment.
	VisitorF func(tags tag.Set, jrnl string) bool
)

const (
	VF_SKIP_IF_LOCKED = 1
	VF_DO_NOT_RELEASE = 2
)
