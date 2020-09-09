package config_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"testing"
	"time"
	"vectorized/pkg/config"

	"github.com/stretchr/testify/require"
)

func makeKey(org string, exp time.Time, overrideHash uint32) (string, error) {
	content := fmt.Sprintf(
		"%s%d%d%d",
		org,
		exp.Year(),
		exp.Month(),
		exp.Day(),
	)
	checksum := uint32(0)
	if overrideHash != 0 {
		checksum = overrideHash
	} else {
		checksum = crc32.ChecksumIEEE([]byte(content))
	}
	lk := &config.LicenseKey{
		Organization:    org,
		ExpirationYear:  uint16(exp.Year()),
		ExpirationMonth: uint8(exp.Month()),
		ExpirationDay:   uint8(exp.Day()),
		Checksum:        checksum,
	}
	bs, err := json.Marshal(lk)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bs), nil
}

func TestCheckLicenseKey(t *testing.T) {
	tests := []struct {
		name           string
		key            func() (string, error)
		expected       bool
		expectedErrMsg string
	}{
		{
			name: "key is valid if the hashes match and the expiration date hasn't passed",
			key: func() (string, error) {
				organization := "vectorized.io"
				expiresAt := time.Now().Add(48 * time.Hour)
				return makeKey(organization, expiresAt, 0)
			},
			expected: true,
		},
		{
			name: "key is invalid if the hashes don't match",
			key: func() (string, error) {
				organization := "vectorized.io"
				expiresAt := time.Now().Add(48 * time.Hour)
				return makeKey(organization, expiresAt, 123)
			},
			expected:       false,
			expectedErrMsg: "Invalid license key",
		},
		{
			name: "key is invalid if the expiration date has already passed",
			key: func() (string, error) {
				organization := "vectorized.io"
				expiresAt := time.Now().Add(-48 * time.Hour)
				return makeKey(organization, expiresAt, 0)
			},
			expected: false,
		},
		{
			name: "key is invalid if it's not base64-encoded",
			key: func() (string, error) {
				return "this is defs not base 64", nil
			},
			expected:       false,
			expectedErrMsg: "Invalid license key",
		},
		{
			name: "key is invalid if it's not valid base64-encoded JSON",
			key: func() (string, error) {
				jsonStr := []byte("{\"thisJson\":\"is invalid\"")
				return base64.StdEncoding.EncodeToString(jsonStr), nil
			},
			expected:       false,
			expectedErrMsg: "Invalid license key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			key, err := tt.key()
			require.NoError(st, err)
			valid, err := config.CheckLicenseKey(key)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
			}
			require.Exactly(st, tt.expected, valid)
		})
	}
}
