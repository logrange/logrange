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

package cursor

import (
	"context"
	"github.com/logrange/logrange/pkg/model"
	"github.com/logrange/logrange/pkg/model/tag"
	"github.com/logrange/range/pkg/records"
	"io"
)

type emptyCursor struct{}

var emptyCur emptyCursor

func (ec emptyCursor) Next(ctx context.Context) {}
func (ec emptyCursor) Get(ctx context.Context) (model.LogEvent, tag.Line, error) {
	return model.LogEvent{}, tag.EmptyLine, io.EOF
}
func (ec emptyCursor) Release()                              {}
func (ec emptyCursor) SetBackward(bool)                      {}
func (ec emptyCursor) CurrentPos() records.IteratorPos       { return "" }
func (ec emptyCursor) Id() uint64                            { return 0 }
func (ec emptyCursor) Offset(ctx context.Context, offs int)  {}
func (ec emptyCursor) ApplyState(state State) error          { return nil }
func (ec emptyCursor) State(context.Context) State           { return State{} }
func (ec emptyCursor) WaitNewData(ctx context.Context) error { return nil }
