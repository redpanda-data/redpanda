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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lestrrat-go/jwx/jwt"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
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
	ClientID string
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
func ValidateToken(token, audience string, clientIDs ...string) error {
	// A missing audience is not validated when using WithAudience below.
	if audience == "" {
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
		jwt.WithAudience(audience))
	if err != nil {
		switch err.Error() {
		case "exp not satisfied":
			return &ExpiredError{parsed.Expiration()}
		case "aud not satisfied":
			return fmt.Errorf("token audience %v does not contain our expected audience %q", parsed.Audience(), audience)
		default:
			return err
		}
	}

	for _, clientID := range clientIDs {
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
	return fmt.Errorf("token client id %q is not our expected client id %q", parsed.PrivateClaims()["azp"], clientIDs)
}

// Client talks to auth0.
type Client struct {
	endpoint Endpoint

	httpCl *httpapi.Client
}

// NewClient initializes and returns a client for talking to auth0.
func NewClient(endpoint Endpoint) *Client {
	opts := []httpapi.Opt{
		httpapi.Host(endpoint.URL),
		httpapi.Err4xx(func(code int) error { return &tokenResponseError{Code: code} }),
	}

	apiCl := httpapi.NewClient(opts...)
	cl := &Client{
		endpoint: endpoint,
		httpCl:   apiCl,
	}

	return cl
}

// Login attempts to receive an auth0 token.
func (cl *Client) Login(ctx context.Context) (token string, expiresIn int, err error) {
	code, err := cl.getDeviceCode(ctx)
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
		token, err := cl.getToken(ctx, code.DeviceCode)
		if err == nil {
			return token.AccessToken, token.ExpiresIn, nil
		}
		var tokenError *tokenResponseError
		if errors.As(err, &tokenError) {
			switch tokenError.Err {
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

func (cl *Client) getDeviceCode(ctx context.Context) (*deviceAuthorizationResponse, error) {
	path := httpapi.Pathfmt("%s/oauth/device/code", cl.endpoint.URL)
	form := httpapi.Values(
		"client_id", cl.endpoint.ClientID,
		"audience", cl.endpoint.Audience,
	)

	var response *deviceAuthorizationResponse
	return response, cl.httpCl.PostForm(ctx, path, nil, form, response)
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
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

// tokenResponseError is the error returned from 4xx responses.
type tokenResponseError struct {
	Code             int    `json:"-"`
	Err              string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// Error implements the error interface.
func (t *tokenResponseError) Error() string {
	if t.ErrorDescription != "" {
		return t.Err + ": " + t.ErrorDescription
	}
	return t.Err
}

func (cl *Client) getToken(ctx context.Context, deviceCode string) (*tokenResponse, error) {
	path := httpapi.Pathfmt("%s/oauth/token", cl.endpoint.URL)
	form := httpapi.Values(
		"grant_type", "urn:ietf:params:oauth:grant-type:device_code",
		"device_code", deviceCode,
		"client_id", cl.endpoint.ClientID,
	)

	var response *tokenResponse
	return response, cl.httpCl.PostForm(ctx, path, nil, form, response)
}
