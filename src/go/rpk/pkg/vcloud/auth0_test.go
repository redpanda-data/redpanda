// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package vcloud_test

import (
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud"
)

const (
	expiredAuth0Token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlZlQVUzVm12OV85b0s5Q0dvbEU5dyJ9.eyJodHRwczovL2Nsb3VkLnZlY3Rvcml6ZWQuaW8vb3JnYW5pemF0aW9uX2lkIjoyLCJodHRwczovL2Nsb3VkLnZlY3Rvcml6ZWQuaW8vcm9sZXMiOlsiQ2xpZW50IEFkbWluIl0sImh0dHBzOi8vY2xvdWQudmVjdG9yaXplZC5pby9wZXJtaXNzaW9ucyI6WyJtYW5hZ2U6b3JnYW5pemF0aW9uLWluZm8iLCJyZWFkOm9yZ2FuaXphdGlvbi1pbmZvIiwid3JpdGU6b3JnYW5pemF0aW9uLWluZm8iXSwiaXNzIjoiaHR0cHM6Ly92ZWN0b3JpemVkLWRldi51cy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTEzNTY5NTI2NTM2MzI2MDk0NjkiLCJhdWQiOiJkZXYudmVjdG9yaXplZC5jbG91ZCIsImlhdCI6MTYxOTQ0MDE1MCwiZXhwIjoxNjE5NTI2NTUwLCJhenAiOiJ5TU1iZkQ2eGRLWFc5RG1JcUFaRHpUQlBIZ2ZJNU15WCIsInBlcm1pc3Npb25zIjpbIm1hbmFnZTpvcmdhbml6YXRpb24taW5mbyIsInJlYWQ6b3JnYW5pemF0aW9uLWluZm8iLCJ3cml0ZTpvcmdhbml6YXRpb24taW5mbyJdfQ.fjafhPoJuoOH84A5_7uXneswtHi0JZ3pOGTJJKQdvJagMdayK4miouKd0lyEi7hHWoINBN2rnyALsC0EKLyrpEjIdTnV26VIhMQcxMQezJy526-vT-jGiZPo8V_RReGq9zMuD-76DJzRI-vJWvIvTmc4TwmL-rGmZFCakinoEKEtGddTH2e4f11MEcMyCqyk5qrGzQXR9yW2QiZHRDG0Wi7uBePIS7gqK7rPOsbGZ7T88SxQeyvzrkgL3EHa66Y4mexo3E_jPTka1Eh7frTMxioZ5EfeLufeS4gWXcmsTYF7Wvz8fEDEib5T5XNLi-xktrAMgXSsLkZEy4oUeBPswQ"
)

var (
	// self-signed private key used to sign JWT for test
	privateKey = []byte(`-----BEGIN CERTIFICATE-----
	MIIFoDCCA4gCCQC6ZH5bd0JeZjANBgkqhkiG9w0BAQsFADCBkTELMAkGA1UEBhMC
	Q1oxFzAVBgNVBAgMDkN6ZWNoIFJlcHVibGljMQ8wDQYDVQQHDAZQcmFndWUxEzAR
	BgNVBAoMClZlY3Rvcml6ZWQxDjAMBgNVBAsMBUNsb3VkMQ8wDQYDVQQDDAZ2Y2xv
	dWQxIjAgBgkqhkiG9w0BCQEWE2FsZW5hQHZlY3Rvcml6ZWQuaW8wHhcNMjEwNDI4
	MTIzNDQ1WhcNMjIwNDI4MTIzNDQ1WjCBkTELMAkGA1UEBhMCQ1oxFzAVBgNVBAgM
	DkN6ZWNoIFJlcHVibGljMQ8wDQYDVQQHDAZQcmFndWUxEzARBgNVBAoMClZlY3Rv
	cml6ZWQxDjAMBgNVBAsMBUNsb3VkMQ8wDQYDVQQDDAZ2Y2xvdWQxIjAgBgkqhkiG
	9w0BCQEWE2FsZW5hQHZlY3Rvcml6ZWQuaW8wggIiMA0GCSqGSIb3DQEBAQUAA4IC
	DwAwggIKAoICAQDDKcx0xDUt03h16WvNSvf67WoigfhSqYVSFlihe+nHzjbL1z/f
	KJez/u1c093eNvLinNRy/MVFp4GBHNzRQi5Q1TP7Zzs7X0ysEjBdQl/t3yB4dHyn
	DNKP9OEsKqJnG/SJQ4Vde0EKEIPjSMZJsru19U8fkxzivQ+a/spdCZ2xca7DijRn
	laN6oCYhInWz7f5nFInNJ92hfp13qCNHD1bNRYul0griQXlXz9x8si8c5CaZHxt9
	hOc2PufbuWhChFxfKP+fXm50Wvgk8M1kp+0TNDnGDmVVCtBimpx7JNuyaLhM9Yw7
	Z5Eb2/JNIriupXGsAbouPKdjqSGMPzTVAQFwCNQR4lFn5Nth06CJR0V23rV17M7M
	hOTCxKQYMSDdqK2ryeesyFLscIxwdLbeTnP+rlMXHSKfeP1NBu6h0IiD5f/7Fm5o
	S1xeYz1cSYE2puIjMcrL2ElbZfexnZaH5pImOeDYq5wwYQtz6UERLFCWooO3pZmP
	/3R/+ew8XL1UsdKFTAHIYv4D5r7T3DtRl0SvMxgCII5SXM95J0ihu2+qRcMZR5P8
	fwdiolOX97f4RxqCNb/CNRg6PBIDqEKK33I43Z+rPWqStkPNOkdCaJgZNvtSQWPn
	zWGEN62KHCPb8AXR2f9Qc9y58m2E2btzs8dETMTVE/OkxYsVawD1fbsNGQIDAQAB
	MA0GCSqGSIb3DQEBCwUAA4ICAQB1nkdWY6qYl1NDQ2szKvkH69HKh9usx0cUyZNG
	0T+zniuE1CR7XNrL5JxAyXqQpfcQbIaDJ3TBxrFo1PotnRurscof/EMV2jNgQaoC
	ADxD4uG8YhxwQAo/oRCfixJ6grykMJijbp56ZK/SF6sdGhz9jmpJvak+p7avKpTW
	P+uilhVX8kMR4HICOm8OttPSkss8jERqzDWbpoaQzRLgHb0YHDMKRdUA1EWuNJNz
	Hnoqm2fN4RC3EJLvsFvLzANNFqRn8OsKQnKagVhN8j2hkP5PYDoOJXWbuP08J6lb
	d8X5/PgVI6xgXT7pzfRXT6GMLRJZgrLANFmi0/hKBAFR949qYeeDI6liKib10FVM
	SqHI9cNBJ2yomQ0rhRXGDhduOYY/uc7VDvj9XwWUDXSyzrEXDuq4sNlOVYz3wI6j
	fNHIc5J3lgdci66rHKqVOsIcg5Wy8FP7FyDz65JiO1cQy5EO4bvzE/WFq1L8rKV+
	w3HB3v2HUDn9tybWyT2/ZZYeYyf6ST3UIYdBJKwy3/32qJfbE2UY/jHSFVlEgAjZ
	BC89YyWZrAmrmSd1lfpd8Qjg3a97n1FhYZI1FFtj70rnGpS85fk36U6iYq1QGzN5
	Iyce9SyoJdoW5BPNxDp0FjWcHBraenFpzuP0bywwBkOKFquwPppxtZ5Csbu7vstH
	KGtjYw==
	-----END CERTIFICATE-----`)
)

func TestVerifyToken_ExpiredAuth0(t *testing.T) {
	client := vcloud.NewDefaultAuth0Client()
	valid, err := client.VerifyToken(expiredAuth0Token)
	if err != nil {
		t.Errorf("expecting no error for token validate, got %v", err)
	}
	if valid {
		t.Errorf("expecting token to be expired and not valid, is valid instead")
	}
}

func TestVerifyToken_NotExpiredGenerated(t *testing.T) {
	token := jwt.New()
	token.Expiration().Add(1 * time.Hour)
	signedToken, err := jwt.Sign(token, jwa.HS256, privateKey)
	if err != nil {
		t.Errorf("error when signing token for test %v", err)
	}

	client := vcloud.NewDefaultAuth0Client()
	valid, err := client.VerifyToken(string(signedToken))
	if err != nil {
		t.Errorf("expecting no error for token validate, got %v", err)
	}
	if !valid {
		t.Errorf("expecting token to be valid, is invalid instead")
	}
}
