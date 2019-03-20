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

package syslog

import (
	"github.com/logrange/logrange/pkg/utils"
	"io"
	"net"
	"testing"
	"time"
)

func TestTCPSyslog(t *testing.T) {
	msgs := make(chan string)
	defer close(msgs)

	addr := "127.0.0.1:22514"
	prot := "tcp"

	ls, err := net.Listen(prot, addr)
	if err != nil {
		t.Fatal("can't start server, err=", err)
	}
	defer ls.Close()
	go runTcpSrv(ls, msgs)

	msg := newMsg("si\nmple syslog test!")
	write(t, msg, prot, addr)
	check(t, Format(msg, true, 5), msgs)

	msg = newMsg("s")
	write(t, msg, prot, addr)
	check(t, Format(msg, false, 5), msgs)
}

func TestUDPSyslog(t *testing.T) {
	msgs := make(chan string)
	defer close(msgs)

	addr := "127.0.0.1:33514"
	prot := "udp"

	udpAddr, err := net.ResolveUDPAddr(prot, addr)
	ls, err := net.ListenUDP(prot, udpAddr)
	if err != nil {
		t.Fatal("can't start server, err=", err)
	}

	defer ls.Close()
	go runUdpSrv(ls, msgs)

	msg := newMsg("si\nmple syslog test!")
	write(t, msg, prot, addr)
	check(t, Format(msg, true, 5), msgs)

	msg = newMsg("s")
	write(t, msg, prot, addr)
	check(t, Format(msg, false, 5), msgs)
}

func check(t *testing.T, exp string, msgs <-chan string) {
	act, ok := <-msgs
	if !ok || act != exp {
		t.Fatal("actual didn't match with expected...")
	}
}

func write(t *testing.T, msg *Message, prot, addr string) {
	cfg := NewDefaultConfig()
	cfg.Protocol = prot
	cfg.RemoteAddr = addr
	cfg.LineLenLimit = utils.IntPtr(5)

	slog, err := NewLogger(cfg)
	if err != nil {
		t.Fatal("can't start client, err=", err)
	}
	err = slog.Write(msg)
	if err != nil {
		t.Fatal("client failed to write, err=", err)
	}
	err = slog.Close()
	if err != nil {
		t.Fatal("failed client close, err=", err)
	}
}

func newMsg(text string) *Message {
	return &Message{
		Severity: SeverityInfo,
		Facility: FacilityLocal6,
		Hostname: "localhost",
		Tag:      "TestSyslog",
		Time:     time.Now(),
		Msg:      text,
	}
}

func runTcpSrv(ls net.Listener, msgs chan<- string) {
	go func(cls net.Listener) {
		for {
			conn, err := ls.Accept()
			if err != nil {
				return
			}
			bb, err := read(conn)
			if err != nil {
				return
			}
			msgs <- string(bb)
		}
	}(ls)
}

func runUdpSrv(ls *net.UDPConn, msgs chan<- string) {
	go func() {
		for {
			bb, err := read(ls)
			if err != nil {
				return
			}
			msgs <- string(bb)
		}
	}()
}

func read(conn io.ReadCloser) ([]byte, error) {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	return buf[:n], err
}
