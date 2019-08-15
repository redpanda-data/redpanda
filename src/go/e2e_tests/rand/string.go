package rand

import (
	"math/rand"
	"time"
)

func StringWithCharset(length int, charset string) string {
	seededRand := rand.New(
		rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, "abcdefghijklmnopqrstuvwxyz"+
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}
