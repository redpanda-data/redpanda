// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package oauth

// TokenResponseError is the error returned from 4xx responses.
type TokenResponseError struct {
	Code             int    `json:"-"`
	Err              string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// Error implements the error interface.
func (t *TokenResponseError) Error() string {
	if t.ErrorDescription != "" {
		return t.Err + ": " + t.ErrorDescription
	}
	return t.Err
}
