package atmosphere

import (
	"crypto/tls"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jrivets/log4g"
)

type testReader struct {
	n  int
	to time.Duration
}

func (tr *testReader) OnRead(r Reader, n int) error {
	log := log4g.GetLogger("testReader")
	log.Info("OnRead(): n=", n)
	b := make([]byte, n)
	for n > 0 {
		ln, err := r.Read(b)
		if err != nil {
			r.ReadResponse(nil)
			return err
		}
		n -= ln
	}
	if tr.to > 0 {
		time.Sleep(tr.to)
	}
	r.ReadResponse(b)
	return nil
}

func newTestReader() *testReader {
	tr := new(testReader)
	return tr
}

func TestReadWrite(t *testing.T) {
	_, d, _, _ := runtime.Caller(0)
	crtsDir := filepath.Dir(d) + "/test_certs/"

	log4g.SetLogLevel("", log4g.INFO)
	tr := newTestReader()
	scfg := &ServerConfig{ListenAddress: ":12345", ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	scfg.Tls = true
	scfg.TransportConfig.LoadX509Files(crtsDir+"server0.crt", crtsDir+"server0.key", "")
	// one-way TLS
	scfg.ClientAuthType = tls.NoClientCert

	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	msg := "Hello test"
	ccfg := &ClientConfig{AccessKey: "test"}
	ccfg.Tls = true
	ccfg.TransportConfig.LoadX509Files(crtsDir+"client0.crt", crtsDir+"client0.key", crtsDir+"chain.pem")
	cl, err := NewClient("127.0.0.1:12345", ccfg)
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()

	var buf [64]byte
	n, err := cl.Write(Message(msg), Message(buf[:]))
	if n <= 0 || err != nil {
		t.Fatal("expected not nil response and no error")
	}
	resp := string(buf[:n])
	if resp != msg {
		t.Fatal("Expected msg=", msg, ", but received resp=", resp)
	}
}

func TestConcurrentWrite(t *testing.T) {
	tr := newTestReader()
	scfg := &ServerConfig{ListenAddress: ":12350", ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	cl, err := NewClient("127.0.0.1:12350", &ClientConfig{AccessKey: "test"})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()

	log := log4g.GetLogger("TestConcurrentWrite")
	var wg sync.WaitGroup
	ech := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var buf [64]byte
			msg := fmt.Sprint("msg", i)
			n, err := cl.Write(Message(msg), Message(buf[:]))
			resp := string(buf[:n])
			log.Info("Sending ", msg, " received ", n, " bytes, resp=", resp, " err=", err)
			if err != nil {
				ech <- err
			}
			if resp != msg {
				ech <- errors.New("read " + resp + " expected " + msg)
			}
		}(i)
	}

	wg.Wait()
	close(ech)
	err, _ = <-ech
	if err != nil {
		t.Fatal("expecting no error ", err)
	}
}

func TestWrongAuth(t *testing.T) {
	time.Sleep(100)
	tr := newTestReader()
	scfg := &ServerConfig{ListenAddress: ":12351", ConnListener: tr, Auth: func(aKey, sKey string) bool { return false }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	cl, err := NewClient("127.0.0.1:12351", &ClientConfig{AccessKey: "test"})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()

	n, err := cl.Write(Message("test"), nil)
	if n > 0 || err == nil {
		t.Fatal("expected not authenticated")
	}
	t.Log("err=", err)
}

func TestHeartbeat(t *testing.T) {
	// if broken, uncomment and watch what is going on
	//	log4g.SetLogLevel("", log4g.TRACE)
	//	defer log4g.SetLogLevel("", log4g.INFO)
	tr := newTestReader()
	scfg := &ServerConfig{ListenAddress: ":12346", ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	w, err := NewClient("127.0.0.1:12346", &ClientConfig{AccessKey: "test", HeartBeatMs: 1})
	if w == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	cl := w.(*client)
	defer cl.Close()

	start := time.Now()
	cnt := 0
	sid := cl.getSessionId()
	for time.Now().Sub(start) < 100*time.Millisecond && cnt < 5 {
		time.Sleep(time.Millisecond)
		sid2 := cl.getSessionId()
		if sid2 != sid {
			sid = sid2
			cnt++
		}
	}

	if cnt < 5 {
		t.Fatal("Expecting heartbeating ")
	}
}

func TestClosedClientInTransaction(t *testing.T) {
	tr := newTestReader()
	tr.to = time.Second
	scfg := &ServerConfig{ListenAddress: ":12345", ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	msg := "Hello test"
	cl, err := NewClient("127.0.0.1:12345", &ClientConfig{AccessKey: "test"})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()
	go func() {
		time.Sleep(10 * time.Millisecond)
		cl.Close()
	}()
	n, err := cl.Write(Message(msg), nil)
	if err == nil {
		t.Fatal("expected an error, but read n=", n)
	}
	t.Log(err)
}

func TestClosedServerInTransaction(t *testing.T) {
	tr := newTestReader()
	tr.to = time.Second
	scfg := &ServerConfig{ListenAddress: ":12347", ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	msg := "Hello test"
	cl, err := NewClient("127.0.0.1:12347", &ClientConfig{AccessKey: "test"})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()
	go func() {
		time.Sleep(10 * time.Millisecond)
		srv.Close()
	}()
	n, err := cl.Write(Message(msg), nil)
	if err == nil {
		t.Fatal("expected an error, but read n=", n)
	}
	t.Log(err)
}

func TestServerShutdown(t *testing.T) {
	tr := newTestReader()
	tr.to = time.Second
	scfg := &ServerConfig{ListenAddress: ":12348", ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	for i := 0; i < 10; i++ {
		cl, err := NewClient("127.0.0.1:12348", &ClientConfig{AccessKey: "test"})
		if cl == nil || err != nil {
			t.Fatal("Expecting client be created ok")
		}
		defer cl.Close()
	}
	s := srv.(*tcp_server)
	start := time.Now()
	for s.srv.clientsCount() < 10 && time.Now().Sub(start) < 50*time.Millisecond {
		time.Sleep(time.Millisecond)
	}
	if s.srv.clientsCount() < 10 {
		t.Fatal("Expecting 10 clients, but ", s.srv.clientsCount())
	}

	s.Close()
	for s.srv.clientsCount() > 0 && time.Now().Sub(start) < 100*time.Millisecond {
		time.Sleep(time.Millisecond)
	}

	if s.srv.clientsCount() > 0 {
		t.Fatal("Expecting 0 clients, but ", s.srv.clientsCount())
	}
}

func TestServerSessTimeout(t *testing.T) {
	tr := newTestReader()
	tr.to = time.Second
	scfg := &ServerConfig{ListenAddress: ":12345", SessTimeoutMs: 50, ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	cl, err := NewClient("127.0.0.1:12345", &ClientConfig{AccessKey: "test"})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()

	s := srv.(*tcp_server)
	start := time.Now()
	for s.srv.clientsCount() == 0 && time.Now().Sub(start) < 25*time.Millisecond {
		time.Sleep(time.Millisecond)
	}
	if s.srv.clientsCount() != 1 {
		t.Fatal("Expecting 1 client, but ", s.srv.clientsCount())
	}

	for s.srv.clientsCount() == 1 && time.Now().Sub(start) < 200*time.Millisecond {
		time.Sleep(time.Millisecond)
	}
	if s.srv.clientsCount() != 0 {
		t.Fatal("Expecting 0 client, but ", s.srv.clientsCount())
	}
}

func TestServerSessTimeoutVsHeartbeat(t *testing.T) {
	tr := newTestReader()
	tr.to = time.Second
	scfg := &ServerConfig{ListenAddress: ":12346", SessTimeoutMs: 50, ConnListener: tr, Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok err=", err)
	}
	defer srv.Close()

	cl, err := NewClient("127.0.0.1:12346", &ClientConfig{AccessKey: "test", HeartBeatMs: 10})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}
	defer cl.Close()

	s := srv.(*tcp_server)
	start := time.Now()
	for s.srv.clientsCount() == 0 && time.Now().Sub(start) < 25*time.Millisecond {
		time.Sleep(time.Millisecond)
	}
	if s.srv.clientsCount() != 1 {
		t.Fatal("Expecting 1 client, but ", s.srv.clientsCount())
	}

	for s.srv.clientsCount() == 1 && time.Now().Sub(start) < 200*time.Millisecond {
		time.Sleep(time.Millisecond)
	}
	if s.srv.clientsCount() != 1 {
		t.Fatal("Expecting 1 client, but ", s.srv.clientsCount())
	}
}
