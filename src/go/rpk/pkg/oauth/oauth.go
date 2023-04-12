package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/lestrrat-go/jwx/jwt"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
)

type (
	// Client is the interface that defines our authorization clients and
	// their authentication flows.
	Client interface {
		// Audience returns the audience used to generate the token.
		Audience() string
		// Token is the access token request using client credentials flow.
		Token(context.Context, cloudcfg.Config) (Token, error)
		// DeviceCode is the device authorization request in the device flow.
		DeviceCode(context.Context, cloudcfg.Config) (DeviceCode, error)
		// DeviceToken is the access token request using device flow.
		DeviceToken(ctx context.Context, cfg cloudcfg.Config, deviceCode string) (Token, error)
		// URLOpener is the utility function to open URLs in the browser for
		// authentication purposes.
		URLOpener(string) error
	}

	// Token is a response for an OAuth 2.0 access token request. The struct
	// follows the RFC6749 definition, for documentation on fields, see sections
	// 4.2.2 and 4.2.2.1:
	//
	// https://datatracker.ietf.org/doc/html/rfc6749#section-4.2.2
	Token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}

	// DeviceCode is a response for an OAuth 2.0 Device Authorization request.
	// The struct follows the RFC8628 definition, section 3.2:
	//
	// https://datatracker.ietf.org/doc/html/rfc8628#section-3.2
	DeviceCode struct {
		DeviceCode              string `json:"device_code"`
		UserCode                string `json:"user_code"`
		VerificationURL         string `json:"verification_uri"`
		VerificationURLComplete string `json:"verification_uri_complete"`
		ExpiresIn               int    `json:"expires_in"`
		Interval                int    `json:"interval"`
	}
)

// ClientCredentialFlow follows the OAuth 2.0 client credential authentication
// flow. First it validates whether the configuration already have a valid
// token.
func ClientCredentialFlow(ctx context.Context, cl Client, cfg *cloudcfg.Config) (Token, error) {
	// We only validate the token if we have the client ID, if one of them is
	// not present we just start the login flow again.
	if cfg.AuthToken != "" && cfg.ClientID != "" {
		expired, err := validateToken(cfg.AuthToken, cl.Audience(), cfg.ClientID)
		if err != nil {
			return Token{}, err
		}
		if !expired {
			return Token{AccessToken: cfg.AuthToken}, nil
		}
	}
	return cl.Token(ctx, *cfg)
}

// DeviceFlow follows the OAuth 2.0 device authentication flow. First it
// validates whether the configuration already have a valid token.
func DeviceFlow(ctx context.Context, cl Client, cfg *cloudcfg.Config) (Token, error) {
	// We only validate the token if we have the client ID, if one of them is
	// not present we just start the login flow again.
	if cfg.AuthToken != "" && cfg.ClientID != "" {
		expired, err := validateToken(cfg.AuthToken, cl.Audience(), cfg.ClientID)
		if err != nil {
			return Token{}, err
		}
		if !expired {
			return Token{AccessToken: cfg.AuthToken}, nil
		}
	}

	dcode, err := cl.DeviceCode(ctx, *cfg)
	if err != nil {
		return Token{}, fmt.Errorf("unable to request the device authorization: %w", err)
	}
	if !isURL(dcode.VerificationURLComplete) {
		return Token{}, fmt.Errorf("authorization server returned an invalid URL: %s; please contact Redpanda support", dcode.VerificationURLComplete)
	}
	err = cl.URLOpener(dcode.VerificationURLComplete)
	if err != nil {
		return Token{}, fmt.Errorf("unable to open the web browser: %v", err)
	}

	fmt.Printf("Opening your browser for authentication, if does not open automatically, please open %q and proceed to login.\n", dcode.VerificationURLComplete)

	token, err := waitForDeviceToken(ctx, cl, *cfg, dcode)
	if err != nil {
		return Token{}, err
	}

	cfg.ClientID = cfg.AuthClientID // if everything succeeded, save the clientID to the one used to generate  the token
	return token, nil
}

func waitForDeviceToken(ctx context.Context, cl Client, cfg cloudcfg.Config, dcode DeviceCode) (Token, error) {
	interval := 5
	if dcode.Interval > 0 {
		interval = dcode.Interval
	}

	// Current Cloud API handler has a fixed expiration time (5min) and
	// does not return ExpiresIn. We default to 6min for our Cloud API, but
	// once we switch to proper Auth0 we will have a real expiresAt.
	expiresAt := time.Now().Add(6 * time.Minute)
	if dcode.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(dcode.ExpiresIn) * time.Second)
	}

	var token Token
	var err error
	for {
		token, err = cl.DeviceToken(ctx, cfg, dcode.DeviceCode)
		if err == nil {
			return token, nil
		}
		if rte := (*TokenResponseError)(nil); errors.As(err, &rte) {
			switch rte.Err {
			case "authorization_pending", "slow_down":
			default:
				return Token{}, fmt.Errorf("unable to request authorization token: %v, please try again or contact support", rte.Err)
			}
		}
		if time.Now().After(expiresAt) {
			return Token{}, fmt.Errorf("failed to retrieve token after %v seconds: timed out waiting for response: %v", expiresAt.String(), err)
		}

		timer := time.NewTimer(time.Duration(interval) * time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return Token{}, fmt.Errorf("failed to retrieve token: %v", ctx.Err())
		}
	}
}

// validateToken validates that the token is valid, not yet expired, it is for
// the given audience, and it is for the given client ID.
//
// If the token is valid, this returns false, nil.
// If the token is expired, this returns true, nil
// Otherwise, this returns false, *BadClientTokenError.
func validateToken(token, audience string, clientIDs ...string) (expired bool, rerr error) {
	defer func() {
		if rerr != nil {
			rerr = &BadClientTokenError{rerr}
		}
	}()
	// A missing audience is not validated when using WithAudience below.
	if audience == "" {
		return false, errors.New("invalid empty audience")
	}

	parsed, err := jwt.Parse([]byte(token))
	if err != nil {
		return false, fmt.Errorf("unable to parse jwt token: %w", err)
	}

	// A missing "exp" field shows up as a zero time.
	if parsed.Expiration().IsZero() {
		return false, errors.New("invalid non-expiring token")
	}

	err = jwt.Validate(parsed,
		jwt.WithAudience(audience))
	if err != nil {
		switch err.Error() {
		case "exp not satisfied":
			return true, nil
		case "aud not satisfied":
			return false, fmt.Errorf("token audience %v does not contain our expected audience %q", parsed.Audience(), audience)
		default:
			return false, err
		}
	}

	for _, clientID := range clientIDs {
		err = jwt.Validate(parsed,
			jwt.WithClaimValue("azp", clientID),
		)
		if err == nil {
			return false, nil
		}
		switch err.Error() {
		case `"azp" not satisfied: values do not match`:
			continue
		default:
			return false, fmt.Errorf("token validation error: %w", err)
		}
	}
	return false, fmt.Errorf("token client id %q is not our expected client id %q", parsed.PrivateClaims()["azp"], clientIDs)
}

func isURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
