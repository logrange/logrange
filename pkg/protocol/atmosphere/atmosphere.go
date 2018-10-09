package atmosphere

import (
	"errors"
)

type (
	// Used for configuring writer (client)
	ClientConfig struct {

		// heartbeat timeout in milliseconds. If set to 0 then no heartbeat
		// will be sent. Values less than several seconds must be used for
		// tests only. Recommended value is 10 seconds
		HeartBeatMs int

		// Access key for the client authentication
		AccessKey string

		// Secret key for the client authentication
		SecretKey string

		TransportConfig
	}

	Message []byte

	// Writer interface is used by clients to send data to the server
	Writer interface {
		// Write sends the message m to the server. Server can respond by a buffer
		// the size of the response is returned in first parameter n, which can be 0.
		// if len(rm) >= n, then the result is stored into rm, otherwise, just
		// the size is returned.
		//
		// If any error happens it is returned in the error and considered like
		// situation when the client cannot be used anymore.
		Write(m Message, rm Message) (int, error)

		Close() error
	}

	// ServerConfig is used for provide configuration to atmosphere server
	ServerConfig struct {
		// For network transports like TCP and TLS provides the addrees where
		// it will be listening on.
		ListenAddress string

		// SessTimeoutMs defines client's session timeout in milliseconds. Server
		// will close all connections who's sessions are not updated for the
		// period of time. If 0, then no timeout is applied. Small values
		// must be used for testing only. Recommended is 30000ms (30sec)
		SessTimeoutMs int

		// ConnListener defines a listener for requests coming from a server
		// connection. See ServerListener.
		ConnListener ServerListener

		// Auth an authenitcation function used by server to authenticate
		// client Auth requests.
		Auth AuthFunc

		TransportConfig
	}

	// Reader used by server to read messages sent by a client. Read and ReadResponse
	// are 2 functions which should be used to read the client message. They are
	// not concurrent and it is user responsibility to not call them from different
	// go routines
	Reader interface {
		// Read put bytes which were sent by client into buf. The method returns
		// number of bytes were actually read. The number can be less than reported
		// in OnRead call. This case the Read operation should be repeated until
		// all data is read. When all bytes were read the client must call
		// ReadResponse() to send response back to the client.
		Read(buf []byte) (int, error)

		// ReadResponse closes the reading process of the client's packet. It can be called
		// before Read() operation read all bytes reported in OnRead() operation.
		// This case the rest bytes will be dropped from the stream and lost forewer.
		//
		// After calling ReadResponse() the Read() may be called only after next
		// OnRead() notification. Violating the rule can broke the connection.
		ReadResponse(resp []byte) error
		Close() error
	}

	// ServerListener registers for the server events
	ServerListener interface {

		// OnRead invoked by atmosphere server to notify listener about a new packet
		// Listener should perform read procedure then following the Reader
		// agreements
		OnRead(r Reader, n int) error
	}

	// AuthFunc is used by server for authentication a client by akey and skey
	// It returns true if aKey and the sKey are knwon and matched
	AuthFunc func(aKey, sKey string) bool
)

var (
	ErrInvalidState = errors.New("the state doesn't allow the operation")
	ErrCannotWrite  = errors.New("cannot perform write, wrong state")
	ErrCannotRead   = errors.New("cannot perform read, wrong state or data unavailable")
	ErrStreamBroken = errors.New("unexpected value is received")
)
