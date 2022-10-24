// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package auth0 provides a client package to talk to auth0.
package auth0

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/lestrrat-go/jwx/jwt"
)

// Endpoint groups what url, audience, and clientID to use for getting tokens.
type Endpoint struct {
	URL      string
	Audience string

	// The API we are accessing. Per the auth0 docs,
	//
	//     The unique identifier of the API your app wants to access. Use the
	//     Identifier value on the Settings tab for the API you created as part
	//     of the prerequisites for this tutorial. Note that this must be URL
	//     encoded.
	//
	ClientID []string
}

// ExpiredError is returned from ValidateToken if the token is already expired.
type ExpiredError struct {
	When time.Time
}

func (e *ExpiredError) Error() string {
	return fmt.Sprintf("token is expired as of %s ago", time.Since(e.When))
}

// ValidateToken validates that the token is valid, not yet expired, it is for
// the given audience, and it is for the given client ID.
//
// If the error is expired, this will return *ExpiredError.
func ValidateToken(token string, endpoint Endpoint) error {
	// A missing audience is not validated when using WithAudience below.
	if endpoint.Audience == "" {
		return errors.New("invalid empty audience")
	}

	parsed, err := jwt.Parse([]byte(token))
	if err != nil {
		return fmt.Errorf("unable to parse jwt token: %w", err)
	}

	// A missing "exp" field shows up as a zero time.
	if parsed.Expiration().IsZero() {
		return errors.New("invalid non-expiring token")
	}

	err = jwt.Validate(parsed,
		jwt.WithAudience(endpoint.Audience))
	if err != nil {
		switch err.Error() {
		case "exp not satisfied":
			return &ExpiredError{parsed.Expiration()}
		case "aud not satisfied":
			return fmt.Errorf("token audience %v does not contain our expected audience %q", parsed.Audience(), endpoint.Audience)
		default:
			return err
		}
	}

	for _, clientID := range endpoint.ClientID {
		err = jwt.Validate(parsed,
			jwt.WithClaimValue("azp", clientID),
		)
		if err == nil {
			return nil
		}
		switch err.Error() {
		case `"azp" not satisfied: values do not match`:
			continue
		default:
			return fmt.Errorf("token validation error: %w", err)
		}
	}
	return fmt.Errorf("token client id %q is not our expected client id %q", parsed.PrivateClaims()["azp"], endpoint.ClientID)
}

// Client talks to auth0.
type Client struct {
	endpoint Endpoint

	httpCl *http.Client
}

// NewClient initializes and returns a client for talking to auth0.
func NewClient(endpoint Endpoint) *Client {
	cl := &Client{
		endpoint: endpoint,
		httpCl:   &http.Client{Timeout: time.Minute},
	}

	// Our auth0 clients are typically extremely short-lived (to issue a single
	// request), so let's be sure not to leak idle connections once the client
	// is GC'd.
	runtime.SetFinalizer(cl, func(cl *Client) {
		cl.httpCl.CloseIdleConnections()
	})

	return cl
}

// Login attempts to receive an auth0 token.
func (cl *Client) Login() (token string, expiresIn int, err error) {
	code, err := cl.getDeviceCode()
	if err != nil {
		return "", 0, fmt.Errorf("unable to login: %w", err)
	}
	fmt.Printf("Please visit the following link to complete your login:\n\n    %s\n",
		code.VerificationURIComplete,
	)

	expiresAt := time.Now().Add(time.Duration(code.ExpiresIn) * time.Second)
	// We add 1 to the interval because, due to clocks and processing and
	// timing, if we tick at the exact interval, we may re-request faster
	// than auth0 updates their systems, and we will get a "slow_down".
	ticker := time.NewTicker(time.Duration(code.Interval+1) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		token, err := cl.getToken(code.DeviceCode)
		if err == nil {
			return token.AccessToken, token.ExpiresIn, nil
		}
		if errors.As(err, &token) {
			switch token.Err {
			case "authorization_pending",
				"slow_down":
			default:
				return "", 0, err
			}
		}
		if time.Now().After(expiresAt) {
			return "", 0, err
		}
	}
	return "",
		0,
		errors.New("rpk bug, please describe how you encountered this at https://github.com/redpanda-data/redpanda/issues/new?assignees=&labels=kind%2Fbug&template=01_bug_report.md")
}

// deviceAuthorizationResponse is a response for an OAuth 2 device
// authorization request. The struct follows the RFC8628 definition, for
// documentation on fields, refer to the following link:
//
//	https://datatracker.ietf.org/doc/html/rfc8628#section-3.2
type deviceAuthorizationResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

func (cl *Client) getDeviceCode() (*deviceAuthorizationResponse, error) {
	resp := new(deviceAuthorizationResponse)
	return resp, cl.doFormJSON(
		"device code",
		cl.endpoint.URL+"/oauth/device/code",
		url.Values{
			"client_id": {cl.endpoint.ClientID[0]},
			"audience":  {cl.endpoint.Audience},
		},
		resp,
	)
}

// tokenResponse is a response for an OAuth 2 access token request. The struct
// follows the RFC6749 definition, for documentation on fields, see sections
// 4.2.2 and 4.2.2.1:
//
//	https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
//
// For a higher-level description, see:
//
//	https://www.oauth.com/oauth2-servers/access-tokens/access-token-response/
type tokenResponse struct { //nolint:errname // This is used as an error once below, but primarily it is not an error.
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`

	Err              string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (t *tokenResponse) Error() string {
	if t.ErrorDescription != "" {
		return t.Err + ": " + t.ErrorDescription
	}
	return t.Err
}

func (cl *Client) getToken(deviceCode string) (*tokenResponse, error) {
	// We specifically go through doForm because a 4xx response for the
	// token request can be JSON that signifies an important error. To us,
	// the error can be significant, because it can say
	// "authorization_pending" which we look for to continue polling.
	body, code, err := cl.doForm(
		"token",
		cl.endpoint.URL+"/oauth/token",
		url.Values{
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
			"device_code": {deviceCode},
			"client_id":   {cl.endpoint.ClientID[0]},
		},
	)
	if err != nil {
		return nil, err
	}

	switch codeBlock := code / 100; codeBlock {
	case 2, 4:
		resp := new(tokenResponse)
		if err := json.Unmarshal(body, resp); err != nil {
			return nil, fmt.Errorf("unable to decode token response body: %w", err)
		}
		if codeBlock == 4 {
			return nil, resp
		}
		return resp, nil

	default:
		return nil, fmt.Errorf("unsuccessful token request: %s", http.StatusText(code))
	}
}

// Issues the input request, unmarhsalling the response into `into`.
//
// All non-2xx responses are considered errors.
func (cl *Client) doFormJSON(name, url string, values url.Values, into interface{}) error {
	body, code, err := cl.doForm(name, url, values)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("unsuccessful %s request: %s\nbody:\n%s\n)", name, http.StatusText(code), body)
	}
	if err := json.Unmarshal(body, into); err != nil {
		return fmt.Errorf("unable to decode %s response body: %w", name, err)
	}
	return nil
}

func (cl *Client) doForm(name, url string, values url.Values) (body []byte, statusCode int, err error) {
	resp, err := cl.httpCl.PostForm(url, values) // nolint:noctx // There is no good replacement for PostForm, and we do not need a context here.
	if err != nil {
		return nil, 0, fmt.Errorf("unable to issue %s request: %w", name, err)
	}
	defer resp.Body.Close()

	if body, err = io.ReadAll(resp.Body); err != nil {
		return nil, 0, fmt.Errorf("unable to read %s response body: %w", name, err)
	}

	return body, resp.StatusCode, nil
}
