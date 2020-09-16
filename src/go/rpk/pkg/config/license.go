package config

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	log "github.com/sirupsen/logrus"
)

type LicenseKey struct {
	Organization    string `json:"o"`
	ExpirationYear  uint16 `json:"y"`
	ExpirationMonth uint8  `json:"m"`
	ExpirationDay   uint8  `json:"d"`
	Checksum        uint32 `json:"c"`
}

// Checks the key and prints a warning if the key is invalid.
func CheckAndPrintNotice(key string) {
	valid, err := CheckLicenseKey(key)
	if err != nil {
		log.Warn(err)
		return
	}
	if !valid {
		log.Warn("Your license key is invalid.")
	}
}

// Checks the license key, which is a Base64-encoded JSON object with the the
// organization it was granted to, the expiration date, and the hash of the
// concatenation of those fields.
// Returns true if the exp. date hasn't passed yet. Returns false otherwise.
// Returns an error if the key isn't base64-encoded or if it's not valid JSON.
func CheckLicenseKey(key string) (bool, error) {
	if key == "" {
		return false, errors.New("Missing license key")
	}
	decoded, err := base64.StdEncoding.DecodeString(key)
	invalidErr := errors.New("Invalid license key")
	if err != nil {
		return false, invalidErr
	}
	lk := &LicenseKey{}
	err = json.Unmarshal([]byte(decoded), lk)
	if err != nil {
		return false, invalidErr
	}

	content := fmt.Sprintf(
		"%s%d%d%d",
		lk.Organization,
		lk.ExpirationYear,
		lk.ExpirationMonth,
		lk.ExpirationDay,
	)

	checksum := crc32.ChecksumIEEE([]byte(content))

	if lk.Checksum != checksum {
		return false, invalidErr
	}

	expDate := time.Date(
		int(lk.ExpirationYear),
		time.Month(lk.ExpirationMonth),
		int(lk.ExpirationDay),
		0, 0, 0, 0,
		time.UTC,
	)

	if time.Now().After(expDate) {
		return false, nil
	}
	return true, nil
}
