package hash

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
)

const (
	secretKeyAlphabet = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnazx_^-()@#$%"
	sessionAlphabet   = "0123456789QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnazx"
)

// GetMacAddress returns a non-loopback interface MAC address. It returns an
// error with the reason, if it is not possible to discover one.
func GetMacAddress() ([]byte, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	var ip string
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() {
			if ipn.IP.To4() != nil {
				ip = ipn.IP.String()
				break
			}
		}
	}
	if ip == "" {
		return nil, fmt.Errorf("could not find any ip address except loopback")
	}

	ifss, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, ifs := range ifss {
		if addrs, err := ifs.Addrs(); err == nil {
			for _, addr := range addrs {
				if strings.Contains(addr.String(), ip) {
					nif, err := net.InterfaceByName(ifs.Name)
					if err != nil {
						continue
					}

					return []byte(nif.HardwareAddr), nil
				}
			}
		}
	}
	return nil, fmt.Errorf("could not find any interface with MAC address")
}

// Makes a session with size characters in the the string. sessionAlphabet
// is used for making the session
func NewSession(size int) string {
	return GetRandomString(size, sessionAlphabet)
}

// Makes a password string
func NewPassword(size int) string {
	return GetRandomString(size, secretKeyAlphabet)
}

func GetRandomString(size int, abc string) string {
	var buffer bytes.Buffer
	var val [64]byte
	var buf []byte
	if size > len(val) {
		buf = make([]byte, size)
	} else {
		buf = val[:size]
	}
	Rand(buf)

	for _, v := range buf {
		buffer.WriteString(string(abc[int(v)%len(abc)]))
	}

	return buffer.String()
}

func Hash(str string) string {
	return BytesHash([]byte(str))
}

func BytesHash(bts []byte) string {
	h := sha256.Sum256(bts)
	return base64.StdEncoding.EncodeToString(h[:])
}

func Rand(bts []byte) {
	if _, err := rand.Read(bts); err != nil {
		panic(err)
	}
}

// Bytes2String transforms bitstream to a string. val contains bytes and
// only lower bits from any value is used for the calculation. abet - is an
// alpabet which is used for forming the result string.
func Bytes2String(val []byte, abet string, bits int) string {
	kap := len(val) * 8 / bits
	abl := len(abet)
	res := make([]byte, 0, kap)
	mask := (1 << uint(bits)) - 1
	i := 0
	shft := 0
	for i < len(val) {
		b := int(val[i]) >> uint(shft)
		bSize := 8 - shft
		if bSize <= bits {
			i++
			if i < (len(val)) {
				shft = bits - bSize
				b |= int(val[i]) << uint(bSize)
			}
		} else {
			shft += bits
		}
		res = append(res, abet[(b&mask)%abl])
	}
	return string(res)
}
