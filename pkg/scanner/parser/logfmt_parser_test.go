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
	"github.com/logrange/logrange/pkg/scanner/model"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

const(
	EmptyLogTestNum int = 1;
	IllformatTestNum1 int = 2;
	IllformatTestNum2 int = 3;
	LessFieldTestNum int = 4;
	EqualFieldTestNum int = 5;
	MoreFieldTestNum int = 6;
	ReplaceTimeTestNum int = 7;

)

/*
test case: log field of record is empty; use record 1 in file
 */
func TestLogfmt_EmptyLog(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	exp, err := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")
	assert.NoError(t, err)

	for i := 0; i < EmptyLogTestNum;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, exp, r.GetDate())
	assert.Equal(t, "stream=stderr", r.Fields)
	for key := range m{
		assert.NotContains(t,r.Fields,key)
	}

}

/*
test case: log field of record is ill-formatted; use record 2 in file
behavior: does not parse out fields
*/

func TestLogfmt_illformatLog1(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	exp, err := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")
	assert.NoError(t, err)


	for i := 0; i < IllformatTestNum1;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, exp, r.GetDate())
	assert.Equal(t, "stream=stderr", r.Fields)
	for key := range m{
		assert.NotContains(t,r.Fields,key) //maybe write out the specific string
	}

}


/*
test case: time in log field of record is ill-formatted; use record 3 in file
time is not parsed out b/c it's illformated but still in vars:fields
*/
func TestLogfmt_illformatLog2(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	exp, err := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")
	assert.NoError(t, err)


	for i := 0; i < IllformatTestNum2;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, exp, r.GetDate())
	assert.Contains(t, r.Fields,"time=2018-01-01T10::49.71369883Z")

}

/*
test case: there are less logfmt field in log field than specified in map; use record 4 in file
*/
func TestLogfmt_lessField(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	for i := 0; i < LessFieldTestNum;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)

	assert.Contains(t, r.Fields, "stream=stderr")
	assert.Contains(t,r.Fields,"type=hello")
	assert.Contains(t,r.Fields,"level=debug")
	assert.NotContains(t,r.Fields,"time=2018-01-01T10:22:49.71369883Z")

}

/*
test case: there are equal number of logfmt field in log field than specified in map; use record 5 in file
*/
func TestLogfmt_equalField(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	for i := 0; i < EqualFieldTestNum;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Contains(t, r.Fields, "stream=stderr")
	assert.Contains(t,r.Fields,"type=hello")
	assert.Contains(t,r.Fields,"level=debug")
	assert.Contains(t,r.Fields,"time=2018-01-01T10:22:49.71369883Z")


}

/*
test case: there are more number of logfmt field in log field than specified in map; use record 6 in file
*/
func TestLogfmt_moreField(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	for i := 0; i < MoreFieldTestNum;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Contains(t, r.Fields, "stream=stderr")
	assert.Contains(t,r.Fields,"type=hello")
	assert.Contains(t,r.Fields,"level=debug")
	assert.Contains(t,r.Fields,"time=2018-01-01T10:22:49.71369883Z")
	assert.NotContains(t,r.Fields,"date=08-12-2019")

}

/*
test case: if there a logfmt time field in log field, then ts is that time; use record 7 in file
*/

func TestLogfmt_replaceTime(t *testing.T) {
	var r *model.Record
	var err error
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	notexp, err := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")
	assert.NoError(t, err)

	exp, err := time.Parse(time.RFC3339Nano, "2018-01-01T10:22:49.71369883Z")
	assert.NoError(t, err)

	for i := 0; i < ReplaceTimeTestNum;i++{
		r, err = jp.NextRecord(context.Background())
	}

	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.NotEqual(t, notexp, r.GetDate())
	assert.Equal(t, exp, r.GetDate())

}

func TestLogfmtReposition(t *testing.T) {
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	pos := jp.GetStreamPos()
	exp, err := jp.NextRecord(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, exp)

	pos2 := jp.GetStreamPos()
	assert.True(t, pos2 > pos)
	err = jp.SetStreamPos(pos)
	assert.NoError(t, err)

	act, err := jp.NextRecord(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, act)
	assert.Equal(t, pos2, jp.GetStreamPos())
	assert.Equal(t, exp, act)

	err = jp.Close()
	assert.NoError(t, err)

}

func TestLogfmtReadAll(t *testing.T) {
	fn := absPath("../testdata/logs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_logfmt-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0f.log")

	var m = map[string]string{
		"time":"time",
		"type": "type",
		"level": "level",
	};

	jp, err := NewLogfmtParser(fn, 4096,m)
	assert.NoError(t, err)

	exp, err := lineCount(fn)
	assert.NoError(t, err)

	act := int64(0)
	for err == nil {
		_, err = jp.NextRecord(context.Background())
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