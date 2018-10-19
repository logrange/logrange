package util

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

func Hash(str string) string {
	return BytesHash([]byte(str))
}

func BytesHash(bts []byte) string {
	h := sha256.Sum256(bts)
	return base64.StdEncoding.EncodeToString(h[:])
}

func Md5(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}
