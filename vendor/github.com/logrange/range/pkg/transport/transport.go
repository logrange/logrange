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

package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"
)

type (
	Config struct {
		TlsEnabled  *bool
		Tls2Way     *bool
		TlsCAFile   string
		TlsKeyFile  string
		TlsCertFile string
		ListenAddr  string
	}

	x509Certs struct {
		keyPair tls.Certificate
		rootPool *x509.CertPool
	}
)

func NewServerListener(cfg Config) (net.Listener, error) {
	if !safeBool(cfg.TlsEnabled) {
		return net.Listen("tcp", cfg.ListenAddr)
	}

	tcfg, err := cfg.getTlsConfig()
	if err != nil {
		return nil, err
	}

	return tls.Listen("tcp", cfg.ListenAddr, tcfg)
}

func NewClientConn(cfg Config) (net.Conn, error) {
	timeout := time.Duration(5) * time.Second
	if !safeBool(cfg.TlsEnabled) {
		return net.DialTimeout("tcp", cfg.ListenAddr, timeout)
	}

	tcfg, err := cfg.getTlsConfig()
	if err != nil {
		return nil, err
	}
	return tls.DialWithDialer(&net.Dialer{
		Timeout: timeout}, "tcp", cfg.ListenAddr, tcfg)
}

//===================== config =====================

func (c *Config) Apply(other *Config) {
	if other.TlsEnabled != nil {
		b := *other.TlsEnabled
		c.TlsEnabled = &b
	}
	if other.Tls2Way != nil {
		b := *other.Tls2Way
		c.Tls2Way = &b
	}
	if other.TlsCertFile != "" {
		c.TlsCertFile = other.TlsCertFile
	}
	if other.TlsKeyFile != "" {
		c.TlsKeyFile = other.TlsKeyFile
	}
	if other.TlsCAFile != "" {
		c.TlsCAFile = other.TlsCAFile
	}
	if other.ListenAddr != "" {
		c.ListenAddr = other.ListenAddr
	}
}

func (c *Config) Check() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("ListenAddr must be non-empty")
	}
	tlsEnabled := safeBool(c.TlsEnabled)
	if tlsEnabled {
		if c.TlsCertFile == "" || c.TlsKeyFile == "" {
			return fmt.Errorf("both TlsCertFile and TlsKeyFile must specified" +
				" when TlsEnabled=%v", tlsEnabled)
		}
	}
	if !tlsEnabled && safeBool(c.Tls2Way) {
		return fmt.Errorf("Tls2Way must 'false' " +
			"when TlsEnabled=%v", tlsEnabled)
	}
	return nil
}

func (c *Config) getTlsConfig() (*tls.Config, error) {
	x509Certs, err := c.readX509Certs()
	if err != nil {
		return nil, err
	}

	certs := make([]tls.Certificate, 0, 1)
	if len(x509Certs.keyPair.Certificate) != 0 {
		certs = append(certs, x509Certs.keyPair)
	}

	roots := x509Certs.rootPool
	if roots == nil {
		roots, err = x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
	}

	auth := tls.NoClientCert
	if safeBool(c.Tls2Way) {
		auth = tls.RequireAndVerifyClientCert
	}

	return &tls.Config{Certificates: certs, RootCAs: roots, ClientAuth: auth, ClientCAs: roots,
		MinVersion: tls.VersionTLS12, PreferServerCipherSuites: true,
		CipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},}, nil
}

func (c *Config) readX509Certs() (*x509Certs, error) {
	certPem, err := readFile(c.TlsCertFile)
	if err != nil {
		return nil, err
	}
	keyPem, err := readFile(c.TlsKeyFile)
	if err != nil {
		return nil, err
	}
	rootPem, err := readFile(c.TlsCAFile)
	if err != nil {
		return nil, err
	}

	x509Certs := new(x509Certs)
	if len(certPem) != 0 && len(keyPem) != 0 {
		x509Certs.keyPair, err = tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			return nil, err
		}
	}
	if len(rootPem) != 0 {
		x509Certs.rootPool = x509.NewCertPool()
		if !x509Certs.rootPool.AppendCertsFromPEM(rootPem) {
			return nil, fmt.Errorf("CA certificates chain is incorrect")
		}
	}
	return x509Certs, err
}

func (c Config) String() string {
	return fmt.Sprint(
		"\n\tTlsEnabled=", safeBool(c.TlsEnabled),
		"\n\tTls2Way=", safeBool(c.Tls2Way),
		"\n\tTlsCAFile=", c.TlsCAFile,
		"\n\tTlsKeyFile=", c.TlsKeyFile,
		"\n\tTlsCertFile=", c.TlsCertFile,
		"\n\tListenAddr=", c.ListenAddr,
	)
}

//===================== utils =====================

func readFile(filename string) ([]byte, error) {
	if len(filename) == 0 {
		return nil, nil
	}
	return ioutil.ReadFile(filename)
}

func safeBool(b *bool) bool {
	return b != nil && *b
}

func boolPtr(b bool) *bool {
	return &b
}
