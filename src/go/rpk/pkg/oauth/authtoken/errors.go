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
)

// BadClientTokenError is returned when the client ID is invalid or some other
// error occurs. This can be used as a hint that the client ID needs to be
// cleared as well.
type BadClientTokenError struct {
	Err error
}

func (e *BadClientTokenError) Error() string {
	return fmt.Sprintf("invalid client token: %v", e.Err)
}

// ErrMissingToken is returned when trying to validate an empty token.
var ErrMissingToken = errors.New("missing cloud token, please login with 'rpk cloud login'")

// ErrExpiredToken is returned when a token has expired.
var ErrExpiredToken = errors.New("cloud token has expired, please login again with 'rpk cloud login'")
