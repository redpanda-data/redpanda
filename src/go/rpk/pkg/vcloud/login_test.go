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
	"strings"
	"testing"

	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud"
)

func TestLogin(t *testing.T) {
	var tests = []struct {
		name           string
		tokenResponse  *vcloud.TokenResponse
		tokenError     error
		deviceResponse *vcloud.DeviceCodeResponse
		deviceError    error

		expectedError string
		expectedToken string
	}{
		{"success",
			&vcloud.TokenResponse{
				AccessToken: "token",
			},
			nil,
			&vcloud.DeviceCodeResponse{
				VerificationURIComplete: "http://uri",
				UserCode:                "123",
				DeviceCode:              "1234",
				Interval:                111,
				ExpiresIn:               789,
			},
			nil,
			"",
			"token"},
	}

	for _, tt := range tests {
		clientMock := &MockedAuth0Client{
			tokenResponse:  tt.tokenResponse,
			tokenError:     tt.tokenError,
			deviceResponse: tt.deviceResponse,
			deviceError:    tt.deviceError,
		}
		token, err := vcloud.Login(clientMock)
		if tt.expectedError != "" && (err == nil || strings.Contains(err.Error(), tt.expectedError)) {
			t.Errorf("%s: got unexpected error %v", tt.name, err)
		}
		if token.Token != tt.expectedToken {
			t.Errorf("%s: got unexpected token. Expecting %s got %s", tt.name, tt.expectedToken, token.Token)
		}
	}
}

type MockedAuth0Client struct {
	tokenResponse  *vcloud.TokenResponse
	tokenError     error
	deviceResponse *vcloud.DeviceCodeResponse
	deviceError    error
}

func (mc *MockedAuth0Client) GetToken(
	deviceCode string,
) (*vcloud.TokenResponse, error) {
	return mc.tokenResponse, mc.tokenError
}
func (mc *MockedAuth0Client) GetDeviceCode(
	audience string,
) (*vcloud.DeviceCodeResponse, error) {
	return mc.deviceResponse, mc.deviceError
}

func (mc *MockedAuth0Client) VerifyToken(token string) (bool, error) {
	return true, nil
}
