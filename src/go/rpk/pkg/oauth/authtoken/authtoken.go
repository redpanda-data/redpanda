// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package authtoken

import (
	"errors"
	"fmt"

	"github.com/lestrrat-go/jwx/v2/jwt"
)

// ValidateToken validates that the token is valid, not yet expired, it is for
// the given audience, and it is for the given client ID.
//
// If the token is valid, this returns false, nil.
// If the token is expired, this returns true, nil
// Otherwise, this returns false, *BadClientTokenError.
func ValidateToken(token, audience string, clientIDs ...string) (expired bool, rerr error) {
	if token == "" {
		return false, ErrMissingToken
	}
	defer func() {
		if rerr != nil {
			rerr = &BadClientTokenError{rerr}
		}
	}()
	// A missing audience is not validated when using WithAudience below.
	if audience == "" {
		return false, errors.New("invalid empty audience")
	}

	parsed, err := jwt.Parse([]byte(token), jwt.WithVerify(false))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired()) {
			return true, nil
		}
		if errors.Is(err, jwt.ErrInvalidAudience()) {
			return false, fmt.Errorf("token audience %v does not contain our expected audience %q", parsed.Audience(), audience)
		}
		return false, fmt.Errorf("unable to parse jwt token: %v", err)
	}

	// A missing "exp" field shows up as a zero time.
	if parsed.Expiration().IsZero() {
		return false, errors.New("invalid non-expiring token")
	}

	err = jwt.Validate(parsed,
		jwt.WithAudience(audience))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired()) {
			return true, nil
		}
		if errors.Is(err, jwt.ErrInvalidAudience()) {
			return false, fmt.Errorf("token audience %v does not contain our expected audience %q", parsed.Audience(), audience)
		}
		return false, fmt.Errorf("token validation error: %v", err)
	}

	for _, clientID := range clientIDs {
		err = jwt.Validate(parsed,
			jwt.WithClaimValue("azp", clientID),
		)
		if err == nil {
			return false, nil
		}
		if err.Error() == `"azp" not satisfied: values do not match` {
			continue
		}
		return false, fmt.Errorf("token validation error: %w", err)
	}
	return false, fmt.Errorf("token client id %q is not our expected client id %q", parsed.PrivateClaims()["azp"], clientIDs)
}

// ValidateTokenOrExpire performs the same validation as in ValidateToken, but
// it returns ErrExpiredToken if the token is expired.
func ValidateTokenOrExpire(token, audience string, clientIDs ...string) error {
	expired, err := ValidateToken(token, audience, clientIDs...)
	if err != nil {
		return err
	}
	if expired {
		return ErrExpiredToken
	}
	return nil
}
