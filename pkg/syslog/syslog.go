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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/logrange/logrange/pkg/utils"
	"github.com/mohae/deepcopy"
	"io"
	"io/ioutil"
	"net"
	"time"
)

type (
	Logger struct {
		cfg     *Config
		rootCAs *x509.CertPool
		conn    net.Conn
	}
)

const (
	ProtoTCP = "tcp"
	ProtoUDP = "udp"
	ProtoTLS = "tls"
)

//===================== logger =====================

func NewLogger(cfg *Config) (*Logger, error) {
	err := cfg.Check()
	if err != nil {
		return nil, fmt.Errorf("invalid config; %v", err)
	}

	l := new(Logger)
	l.cfg = deepcopy.Copy(cfg).(*Config)

	if _, ok := utils.PtrBool(cfg.ReplaceNewLine); !ok {
		l.cfg.ReplaceNewLine = utils.BoolPtr(false)
	}
	if _, ok := utils.PtrInt(cfg.LineLenLimit); !ok {
		l.cfg.LineLenLimit = utils.IntPtr(0)
	}
	if _, ok := utils.PtrInt(cfg.ConnectTimeoutSec); !ok {
		l.cfg.ConnectTimeoutSec = utils.IntPtr(0)
	}
	if _, ok := utils.PtrInt(cfg.WriteTimeoutSec); !ok {
		l.cfg.WriteTimeoutSec = utils.IntPtr(0)
	}

	err = l.loadRootCA()
	if err != nil {
		return nil, err
	}

	return l, l.connect()
}

func (l *Logger) Close() error {
	if l.conn != nil {
		return l.conn.Close()
	}
	return nil
}

func (l *Logger) Write(msg *Message) error {
	var (
		err error
	)

	if l.conn == nil {
		err = l.connect()
		if err != nil {
			return err
		}
	}

	timeout := time.Now().Add(time.Second *
		time.Duration(*l.cfg.WriteTimeoutSec))

	err = l.conn.SetWriteDeadline(timeout)
	if err == nil {
		_, err = io.WriteString(l.conn, Format(msg,
			*l.cfg.ReplaceNewLine, *l.cfg.LineLenLimit))
	}

	if err != nil {
		_ = l.Close()
	}

	return err
}

func (l *Logger) loadRootCA() error {
	if l.cfg.RootCAFile != "" {
		rootPem, err := ioutil.ReadFile(l.cfg.RootCAFile)
		if err != nil {
			return err
		}
		l.rootCAs = x509.NewCertPool()
		if !l.rootCAs.AppendCertsFromPEM(rootPem) {
			return fmt.Errorf("CA certificates chain is incorrect")
		}
	}
	return nil
}

func (l *Logger) connect() error {
	var (
		config *tls.Config
		err    error
	)

	if l.conn != nil {
		_ = l.Close()
	}

	timeout := time.Second *
		time.Duration(*l.cfg.ConnectTimeoutSec)

	if l.cfg.Protocol == ProtoTLS {
		if l.rootCAs != nil {
			config = &tls.Config{RootCAs: l.rootCAs}
		}
		dialer := &net.Dialer{
			Timeout: timeout,
		}
		l.conn, err = tls.DialWithDialer(dialer, l.cfg.Protocol, l.cfg.RemoteAddr, config)
		return err
	}

	l.conn, err = net.DialTimeout(l.cfg.Protocol, l.cfg.RemoteAddr, timeout)
	return err
}
