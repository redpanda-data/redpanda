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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/lestrrat-go/jwx/jwt"
)

type TokenResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	AccessToken      string `json:"access_token"`
	ExpiresIn        int    `json:"expires_in"`
}

// deviceCodeResponse holds information about the authorization
// reference https://github.com/cli/oauth/blob/6b1e71c3614ec61205f1ffc9964b06dd61221385/device/device_flow.go#L39
type DeviceCodeResponse struct {
	// code to be entered into the browser by user
	UserCode string `json:"user_code"`
	// url user is asked to open for authorization
	VerificationURI string `json:"verification_uri"`
	// url for user to authorize including user code
	VerificationURIComplete string `json:"verification_uri_complete"`

	// The device verification code is 40 characters and used to verify the device.
	DeviceCode string `json:"device_code"`
	// The number of seconds before the deviceCode and userCode expire.
	ExpiresIn int `json:"expires_in"`
	// The minimum number of seconds that must pass before you can make a new access token request to
	// complete the device authorization.
	Interval int `json:"interval"`
}

type AuthClient interface {
	GetToken(deviceCode string) (*TokenResponse, error)
	GetDeviceCode(audience string) (*DeviceCodeResponse, error)
	VerifyToken(token string) (bool, error)
}

type Auth0Client struct {
	clientId    string
	auth0Domain string
}

var (
	// auth0 clientId
	// TODO(av) replace default with production clientId
	clientId string = "yMMbfD6xdKXW9DmIqAZDzTBPHgfI5MyX"
)

const (
	// TODO(av) should be configurable
	defaultAuth0Domain = "vectorized-dev.us.auth0.com"

	// url path to retrieve device code
	deviceCodePath = "oauth/device/code"
	// url path to retrieve token
	tokenPath = "oauth/token"
)

func NewDefaultAuth0Client() AuthClient {
	return &Auth0Client{
		clientId:    clientId,
		auth0Domain: defaultAuth0Domain,
	}
}

// getToken queries auth0 token endpoint waiting for token to be issued
// https://auth0.com/docs/flows/call-your-api-using-the-device-authorization-flow#request-tokens
func (ac *Auth0Client) GetToken(deviceCode string) (*TokenResponse, error) {
	requestBody := []byte(fmt.Sprintf("grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=%s&client_id=%s", deviceCode, ac.clientId))
	deviceCodeUrl := fmt.Sprintf("https://%s/%s", defaultAuth0Domain, tokenPath)
	req, err := http.NewRequest(http.MethodPost, deviceCodeUrl, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("error creating token request. %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling token api: %w", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading token response body: %w", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code. 200 != %d. Body: %s", resp.StatusCode, body)
	}
	var response TokenResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling token response: %w", err)
	}
	return &response, nil
}

func (ac *Auth0Client) GetDeviceCode(
	audience string,
) (*DeviceCodeResponse, error) {
	requestBody := []byte(fmt.Sprintf("client_id=%s&audience=%s", ac.clientId, yakAudience))
	deviceCodeUrl := fmt.Sprintf("https://%s/%s", defaultAuth0Domain, deviceCodePath)
	req, err := http.NewRequest(http.MethodPost, deviceCodeUrl, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("error creating device code request. %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling device code api: %w", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading device code step body: %w", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code. 200 != %d. Body: %s", resp.StatusCode, body)
	}
	var response DeviceCodeResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling the device code response: %w", err)
	}
	return &response, nil
}

func (ac *Auth0Client) VerifyToken(token string) (bool, error) {
	parsed, err := jwt.Parse([]byte(token))
	if err != nil {
		return false, fmt.Errorf("error parsing jwt token. %w", err)
	}
	return jwt.Validate(parsed) == nil, nil
}
