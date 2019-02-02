package atmosphere

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func runTestEchoSrv(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Oops got the err=", err, " but seems it is ok, cause test probably already passed")
			return
		}
		go handleTestEchoConn(conn)
	}
}

func handleTestEchoConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		msg, err := r.ReadString('\n')
		if err != nil {
			fmt.Println("handleTestEchoConn: Error while reading, err=", err)
			return
		}

		n, err := conn.Write([]byte(msg))
		if err != nil || n != len(msg) {
			fmt.Println("handleTestEchoConn: Error while writing, err=", err)
			return
		}
	}
}

func TestConnectivity(t *testing.T) {
	cl, err := NewClient("127.0.0.1:12345", &ClientConfig{})
	if cl != nil || err == nil {
		t.Fatal("1) Expecting error for the connection")
	}

	scfg := &ServerConfig{ListenAddress: ":12345", Auth: func(aKey, sKey string) bool { return true }}
	srv, err := NewServer(scfg)
	if srv == nil || err != nil {
		t.Fatal("should create srv ok")
	}

	cl, err = NewClient("127.0.0.1:12345", &ClientConfig{})
	if cl == nil || err != nil {
		t.Fatal("Expecting client be created ok")
	}

	srv.Close()
	cl.Close()
	cl, err = NewClient("127.0.0.1:12345", &ClientConfig{})
	if cl != nil || err == nil {
		t.Fatal("2) Expecting error for the connection")
	}
}

func TestTlsMutual(t *testing.T) {
	_, d, _, _ := runtime.Caller(0)
	crtsDir := filepath.Dir(d) + "/test_certs/"
	fmt.Println(crtsDir)

	scfg := &ServerConfig{ListenAddress: "127.0.0.1:12347"}
	scfg.Tls = true
	scfg.TransportConfig.LoadX509Files(crtsDir+"server0.crt", crtsDir+"server0.key", crtsDir+"chain.pem")
	scfg.ClientAuthType = tls.RequireAndVerifyClientCert
	ln, err := getServerListener(scfg)
	if err != nil {
		t.Fatal("Oops could not create server listener: err=", err)
	}
	defer ln.Close()
	switch netln := ln.(type) {
	case *net.TCPListener:
		fmt.Println("alsfjkhalfjdlasdfjlasjf: *TCPListener ", scfg)
	case nil:
		fmt.Println("alsfjkhalfjdlasdfjlasjf:nil ")
	default:

		r := reflect.ValueOf(ln)
		f := reflect.Indirect(r).FieldByName("Listener")

		if fl, ok := f.Interface().(*net.TCPListener); ok {
			fmt.Println("Fucking shit ", reflect.TypeOf(fl))
		}
		fmt.Println("YYyyyyyYYYYYY: ", reflect.TypeOf(netln).Name(), " pp ", reflect.TypeOf(netln).PkgPath(), " ss ", reflect.TypeOf(netln).String())
	}



	go runTestEchoSrv(ln)

	ccfg := &ClientConfig{}
	ccfg.Tls = true
	ccfg.TransportConfig.LoadX509Files(crtsDir+"client0.crt", crtsDir+"client0.key", crtsDir+"chain.pem")
	cc, err := getClientConn("127.0.0.1:12347", ccfg)
	if err != nil {
		t.Fatal("Oops could not create client listener: err=", err)
	}

	msg := "Hello TLS!\n"
	n, err := cc.Write([]byte(msg))
	if err != nil {
		t.Fatal("Oops could not write to client, err=", err)
	}

	buf := make([]byte, 100)
	n, err = cc.Read(buf)
	if err != nil || len(msg) != n || string(buf[:n]) != msg {
		t.Fatal("Oops could not read whatever we sent, err=", err, ", n=", n, ", msg=", msg, ", resp=", string(buf[:n]))
	}
}
