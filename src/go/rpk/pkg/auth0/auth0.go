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

// TokenResponse is a response for an OAuth 2 access token request. The struct
// follows the RFC6749 definition, for documentation on fields, see sections
// 4.2.2 and 4.2.2.1:
//
//	https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
//
// For a higher-level description, see:
//
//	https://www.oauth.com/oauth2-servers/access-tokens/access-token-response/
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type getAuthUrlRequest struct {
	ClientId string `json:"client_id"`
}

type getAuthUrlResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationUri         string `json:"verification_uri"`
	VerificationUriComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
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

func (cl *Client) GetToken(ctx context.Context, clientID, clientSecret string) (TokenResponse, error) {
	path := "/oauth/token"
	form := httpapi.Values(
		"grant_type", "client_credentials",
		"client_id", clientID,
		"client_secret", clientSecret,
		"audience", cl.endpoint.Audience,
	)

	var response TokenResponse
	return response, cl.httpCl.PostForm(ctx, path, nil, form, &response)
}

func (cl *Client) InitDeviceAuthorization(ctx context.Context, clientID string) (getAuthUrlResponse, error) {
	path := "/oauth/device/code"
	body := &getAuthUrlRequest{
		ClientId: clientID,
	}

	var response getAuthUrlResponse
	return response, cl.httpCl.Post(ctx, path, nil, "application/json", body, &response)
}

func (cl *Client) WaitForToken(ctx context.Context, deviceCode string, authAppClientId string) (chan TokenResponse, chan string) {
	rc := make(chan TokenResponse)
	rec := make(chan string)

	path := "/oauth/token"
	ct := "application/json"
	body := struct {
		ClientId   string `json:"client_id"`
		DeviceCode string `json:"device_code"`
		GrantType  string `json:"grant_type"`
	}{
		ClientId:   authAppClientId,
		DeviceCode: deviceCode,
		GrantType:  "urn:ietf:params:oauth:grant-type:device_code",
	}

	var response TokenResponse
	var getToken func()
	getToken = func() {
		err := cl.httpCl.Post(ctx, path, nil, ct, body, &response)
		if err == nil {
			rc <- response
			return
		}
		switch e := err.(type) {
		case *tokenResponseError:
			switch e.Err {
			case "authorization_pending":
				go getToken()
				return
			case "invalid_request":
				rec <- "invalid request, please contact support"
			case "invalid_grant":
				rec <- "invalid grant request, please contact support"
			case "unauthorized_client":
				rec <- "unauthorized client"
			case "expired_token":
				rec <- "expired has token"
			default:
				rec <- "server error, please try again, if it keeps failing please contact support"
			}
		default:
			rec <- "Error on getting token, please try again"
		}
	}
	go getToken()
	return rc, rec
}
