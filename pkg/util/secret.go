package util

const (
	secretKeyAlphabet = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnazx_^-()@#$%"
	sessionAlphabet   = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnazx"
)

// Makes a session with size characters in the the string. sessionAlphabet
// is used for making the session
func NewSessionId(size int) string {
	return GetRandomString(size, sessionAlphabet)
}

// Makes a password string
func NewPassword(size int) string {
	return GetRandomString(size, secretKeyAlphabet)
}
