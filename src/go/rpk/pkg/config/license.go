// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
	err := CheckLicenseKey(key)
	if err != nil {
		log.Info(err)
		return
	}
}

// Checks the license key, which is a Base64-encoded JSON object with the the
// organization it was granted to, the expiration date, and the hash of the
// concatenation of those fields.
// Returns true if the exp. date hasn't passed yet. Returns false otherwise.
// Returns an error if the key isn't base64-encoded or if it's not valid JSON.
func CheckLicenseKey(key string) error {
	msg := "Please get a new one at https://vectorized.io/download-trial/"
	if key == "" {
		return errors.New("Missing license key. " + msg)
	}
	decoded, err := base64.StdEncoding.DecodeString(key)
	invalidErr := errors.New("Invalid license key. " + msg)
	if err != nil {
		return invalidErr
	}
	lk := &LicenseKey{}
	err = json.Unmarshal(decoded, lk)
	if err != nil {
		return invalidErr
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
		return invalidErr
	}

	expDate := time.Date(
		int(lk.ExpirationYear),
		time.Month(lk.ExpirationMonth),
		int(lk.ExpirationDay),
		0, 0, 0, 0,
		time.UTC,
	)

	if time.Now().After(expDate) {
		return errors.New("Your license key has expired. " + msg)
	}
	return nil
}
