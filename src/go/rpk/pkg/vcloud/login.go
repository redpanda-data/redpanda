// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package vcloud

import (
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// audience of yak API in Auth0
	// TODO(av) make it configurable to be able to work with production as well
	yakAudience = "dev.vectorized.cloud"
)

var (
	ErrTimeout = errors.New("authentication timed out")
)

type AuthToken struct {
	Token string
}

// Login implements device flow of oauth2
// reference https://auth0.com/docs/flows/call-your-api-using-the-device-authorization-flow
func Login(auth0Client AuthClient) (*AuthToken, error) {
	deviceCodeResponse, err := auth0Client.GetDeviceCode(yakAudience)
	if err != nil {
		return nil, err
	}

	log.Info(fmt.Sprintf("Go to the following link in your browser (code: %s):\n", deviceCodeResponse.UserCode))
	log.Info(deviceCodeResponse.VerificationURIComplete)

	log.Info("\nWaiting for authorization token to be issued...")
	token, err := waitForToken(auth0Client, deviceCodeResponse)
	if err != nil {
		return nil, err
	}
	return &AuthToken{token.AccessToken}, nil
}

func waitForToken(
	auth0Client AuthClient, deviceCode *DeviceCodeResponse,
) (*TokenResponse, error) {
	checkInterval := time.Duration(deviceCode.Interval) * time.Second
	expiresAt := time.Now().Add(time.Duration(deviceCode.ExpiresIn) * time.Second)
	for {
		time.Sleep(checkInterval)
		token, err := auth0Client.GetToken(deviceCode.DeviceCode)
		if err == nil {
			return token, nil
		}
		log.Debug(err)
		if time.Now().After(expiresAt) {
			return nil, ErrTimeout
		}
	}
}
