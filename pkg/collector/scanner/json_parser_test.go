package scanner

import (
	"context"
	"io"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestKnown(t *testing.T) {
	_, d, _, _ := runtime.Caller(0)
	tstFile := filepath.Dir(d) + "/testdata/tlogs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_dnsmasq-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0e.log"

	p, err := newJsonParser(tstFile, 1000000, context.Background())
	if err != nil {
		t.Fatal("Should be able to create the json parser, but got err=", err, " tstFile=", tstFile)
	}
	defer p.Close()

	tm, _ := time.Parse(time.RFC3339Nano, "2018-02-06T10:22:49.201168034Z")

	r, err := p.NextRecord()
	if err != nil {
		t.Fatal("should be able to parse")
	}

	if r.GetTs() != tm {
		t.Fatal("Expecting ", tm, " but got ", r.GetTs())
	}

	pos := p.GetStreamPos()
	r2, err := p.NextRecord()
	pos2 := p.GetStreamPos()
	if err != nil || pos2 <= pos {
		t.Fatal("Could not read second record, or wrong pos is returned err=", err, ", pos=", pos, ", r2 pos2=", pos2)
	}

	p.SetStreamPos(pos)
	r22, err := p.NextRecord()
	if err != nil || pos2 != p.GetStreamPos() {
		t.Fatal("second record re-read, or err=", err, " pos2=", pos2, ", but returned ", p.GetStreamPos())
	}

	if !reflect.DeepEqual(r2, r22) {
		t.Fatal("r2=", r2, " is not the same as ", r22)
	}
}

func TestReadAll(t *testing.T) {
	_, d, _, _ := runtime.Caller(0)
	tstFile := filepath.Dir(d) + "/testdata/tlogs/k8s/var/log/containers/kube-dns-5c47645d88-p7fpl_kube-system_dnsmasq-74d2de2d0542c5b507e57d5077832b2400273554c3c16d674f1302bc010d5a0e.log"

	p, err := newJsonParser(tstFile, 1000000, context.Background())
	if err != nil {
		t.Fatal("Should be able to create the json parser, but got err=", err, " tstFile=", tstFile)
	}
	defer p.Close()

	cnt := 0
	for err == nil {
		_, err = p.NextRecord()
		cnt++
	}

	if err != io.EOF || cnt != 17 {
		t.Fatal("expection io.EOF and 17 records, but err=", err, " and cnt=", cnt)
	}
}
