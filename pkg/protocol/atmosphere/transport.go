package atmosphere

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
)

type (
	TransportConfig struct {
		// Tls specifies whether TLS 1.2 must be used or everything should be
		// on top of pure TCP connection
		Tls bool

		// ClientAuthType specifies the client auth type
		ClientAuthType tls.ClientAuthType

		// KeyPemBlock contains data from private key PEM file.
		KeyPemBlock []byte

		// CertPemBlock contains data from certificate PEM file
		CertPemBlock []byte

		// RootPemBlock contains data from CA PEM file with certificates chain
		RootPemBlock []byte
	}

	// tcp_server handles either TCP or TLS atmosphere connections
	tcp_server struct {
		ln  net.Listener
		srv *server
	}
)

// NewClient creates atmosphere client over pure TCP or TLS 1.2 transport
func NewClient(srvAddrs string, ccfg *ClientConfig) (Writer, error) {
	conn, err := getClientConn(srvAddrs, ccfg)
	if err != nil {
		return nil, err
	}

	return newClient(ccfg, conn), nil
}

// NewClient creates atmosphere server over pure TCP or TLS 1.2 transport
func NewServer(scfg *ServerConfig) (io.Closer, error) {
	srv := newServer(scfg)
	ln, err := getServerListener(scfg)
	if err != nil {
		return nil, err
	}

	go func() {
		defer srv.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				break
			}
			err = srv.Serve(conn)
			if err != nil {
				break
			}
		}
	}()
	return &tcp_server{ln, srv}, nil
}

// LoadX509Files reads cert, key and CA chain PEM files and populates CertPemBlock,
// KeyPemBlock and RootPemBlock accordingly. It will ignore data if the filename
// is empty ("").
func (tc *TransportConfig) LoadX509Files(cert, key, chain string) (err error) {
	tc.CertPemBlock, err = readFile(cert)
	if err != nil {
		return err
	}

	tc.KeyPemBlock, err = readFile(key)
	if err != nil {
		return err
	}

	tc.RootPemBlock, err = readFile(chain)
	return err
}

// readFile reads the file content and returns it as a slice of bytes, or
// returns nil with error if any.
func readFile(filename string) ([]byte, error) {
	if len(filename) == 0 {
		return nil, nil
	}
	return ioutil.ReadFile(filename)
}

func (tc *TransportConfig) getKeyPairCerts() ([]tls.Certificate, error) {
	if len(tc.KeyPemBlock) != 0 && len(tc.CertPemBlock) != 0 {
		cert, err := tls.X509KeyPair(tc.CertPemBlock, tc.KeyPemBlock)
		if err != nil {
			return []tls.Certificate{}, err
		}
		return []tls.Certificate{cert}, nil
	}
	return []tls.Certificate{}, nil
}

func (tc *TransportConfig) getRootCertPool() (*x509.CertPool, error) {
	if len(tc.RootPemBlock) != 0 {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(tc.RootPemBlock) {
			return nil, fmt.Errorf("Could not create Root Cert Pool, probably the CA certificates chain is incorrect")
		}
		return roots, nil
	}
	return x509.SystemCertPool()
}

func (tc *TransportConfig) getTlsConfig() (*tls.Config, error) {
	kpCerts, err := tc.getKeyPairCerts()
	if err != nil {
		return nil, err
	}

	roots, err := tc.getRootCertPool()
	if err != nil {
		return nil, err
	}

	return &tls.Config{Certificates: kpCerts, RootCAs: roots, ClientAuth: tc.ClientAuthType, ClientCAs: roots}, nil
}

func (ts *tcp_server) Close() error {
	return ts.ln.Close()
}

func getServerListener(scfg *ServerConfig) (net.Listener, error) {
	if !scfg.Tls {
		return net.Listen("tcp", scfg.ListenAddress)
	}

	tcfg, err := scfg.getTlsConfig()
	if err != nil {
		return nil, err
	}

	return tls.Listen("tcp", scfg.ListenAddress, tcfg)
}

func getClientConn(srvAddrs string, ccfg *ClientConfig) (net.Conn, error) {
	if !ccfg.Tls {
		return net.Dial("tcp", srvAddrs)
	}

	tcfg, err := ccfg.getTlsConfig()
	if err != nil {
		return nil, err
	}

	return tls.Dial("tcp", srvAddrs, tcfg)
}
