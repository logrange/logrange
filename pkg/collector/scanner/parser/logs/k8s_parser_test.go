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

package logs

import (
	"context"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

func TestOne(t *testing.T) {
	fn := absPath("../../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_dnsmasq-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0e.log")
	jp, err := NewK8sJsonParser(fn, 1<<12, context.Background())
	assert.NoError(t, err)

	exp, err := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")
	assert.NoError(t, err)

	r, err := jp.NextRecord()
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, exp, r.GetDate())
	assert.Equal(t, "stderr", r.GetMeta("stream"))
}

func TestReposition(t *testing.T) {
	fn := absPath("../../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_dnsmasq-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0e.log")
	jp, err := NewK8sJsonParser(fn, 1<<12, context.Background())
	assert.NoError(t, err)

	pos := jp.GetStreamPos()
	exp, err := jp.NextRecord()
	assert.NoError(t, err)
	assert.NotNil(t, exp)

	pos2 := jp.GetStreamPos()
	assert.True(t, pos2 > pos)
	err = jp.SetStreamPos(pos)
	assert.NoError(t, err)

	act, err := jp.NextRecord()
	assert.NoError(t, err)
	assert.NotNil(t, act)
	assert.Equal(t, pos2, jp.GetStreamPos())
	assert.Equal(t, exp, act)

	err = jp.Close()
	assert.NoError(t, err)
}

func TestReadAll(t *testing.T) {
	fn := absPath("../../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_dnsmasq-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0e.log")
	jp, err := NewK8sJsonParser(fn, 1<<12, context.Background())
	assert.NoError(t, err)

	exp, err := lineCount(fn)
	assert.NoError(t, err)

	act := int64(0)
	for err == nil {
		_, err = jp.NextRecord()
		if err == nil {
			act++
		}
	}
	assert.EqualError(t, err, io.EOF.Error())
	assert.Equal(t, int64(exp), act)
	assert.Equal(t, jp.GetStats().FileStats.Size, jp.GetStats().FileStats.Pos)

	err = jp.Close()
	assert.NoError(t, err)
}
