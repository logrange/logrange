// atmosphere package provides implementation for simple client-server protocol
// for sending log data to an aggregation server.
package atmosphere

import (
	"fmt"
)

type (
	// AuthReq is sent by client with ptAuth, to request authentication. The same
	// message is used by client for sending heartbeat, using session id instead
	// of keys.
	AuthReq struct {
		// AccessKey contains the access key for the authentication. It could be nil
		// or empty in case of sessionId is provided
		AccessKey *string `json:"ak,omitempty"`

		// SecretKey contains the secret key for the authentication. It could be nil
		// or empty in case of sessionId is provided
		SecretKey *string `json:"sk,omitempty"`

		// SessionId contains a session id for provide the authentication.
		// This method is used for sending a hearbeat
		SessionId *string `json:"sessId,omitempty"`
	}

	// AuthResp is a response sent by server back to the client as a response
	// to AuthReq.
	AuthResp struct {
		// SessionId contains session id that should be used for heartbeats
		SessionId *string `json:"sessId,omitempty"`

		// Terminal contains whether the client can perform attempts for authentication
		// or not. If the field is true, client must close the connection and
		// reopen it with other params. Server will close the connection soon, if
		// client will not do this.
		Terminal bool `json:"terminal"`

		// Informational message regarding the authentication problem
		Info *string `json:"info,omitempty"`
	}
)

var (
	ptAuth    = 1
	ptMessage = 2
)

func strPtr(s string) *string {
	return &s
}

func strFromPtr(ps *string) string {
	if ps == nil {
		return ""
	}
	return *ps
}

func (ar *AuthResp) String() string {
	return fmt.Sprint("{SessionId=", strFromPtr(ar.SessionId), ", Terminal=", ar.Terminal,
		", Info=", strFromPtr(ar.Info), "}")
}

func (ar *AuthReq) String() string {
	return fmt.Sprint("{AccessKey=", strFromPtr(ar.AccessKey), ", SecretKey=", len(strFromPtr(ar.SecretKey)),
		", SessionId=", strFromPtr(ar.SessionId), "}")
}
