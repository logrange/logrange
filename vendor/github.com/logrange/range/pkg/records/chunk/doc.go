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

/*
Package chunk contains Chunk interface which defines functions for accessing
to a chunk. Chunk is a an ordered set of records which are stored somewhere and
which could be iterated over. Using the Chunk interface and the chunk Iterator,
a client code can receive access to the set of records, add new ones and walk
over them.

Every chunk has an unique identifier Id. The Ids are sortable, so chunks can
be ordered by using their Ids

*/
package chunk
