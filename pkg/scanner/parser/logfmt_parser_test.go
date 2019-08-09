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

package parser

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)




func TestParsingLog(t *testing.T) {
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_dnsmasq-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0e.log")
	m := map[string]string{
		"time":"",
		"type": "",
		"level": "",
		"traceid": "",
	}
	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	exp, err := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")
	assert.NoError(t, err)


	r, err := jp.NextRecord(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, exp, r.GetDate())
	assert.Equal(t, "stream=stderr", r.Fields)
}