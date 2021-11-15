// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"errors"
	"net/http"
	"net/url"
)

const usersEndpoint = "/v1/security/users"

type newUser struct {
	User      string `json:"username"`
	Password  string `json:"password"`
	Algorithm string `json:"algorithm"`
}

const (
	// Redpanda supports only SCRAM at the moment, which has two varieties.
	//
	// Both of the below technically go against the Go naming conventions
	// for acronyms, but 8 uppercase letters for two merged acronyms is a
	// bit odd.
	ScramSha256 = "SCRAM-SHA-256"
	ScramSha512 = "SCRAM-SHA-512"
)

// CreateUser creates a user with the given username and password using the
// given mechanism (SCRAM-SHA-256, SCRAM-SHA-512).
func (a *AdminAPI) CreateUser(username, password, mechanism string) error {
	if username == "" {
		return errors.New("invalid empty username")
	}
	if password == "" {
		return errors.New("invalid empty password")
	}
	u := newUser{
		User:      username,
		Password:  password,
		Algorithm: mechanism,
	}
	return a.sendAny(http.MethodPost, usersEndpoint, u, nil)
}

// DeleteUser deletes the given username, if it exists.
func (a *AdminAPI) DeleteUser(username string) error {
	if username == "" {
		return errors.New("invalid empty username")
	}
	path := usersEndpoint + "/" + url.PathEscape(username)
	return a.sendAny(http.MethodDelete, path, nil, nil)
}

// ListUsers returns the current users.
func (a *AdminAPI) ListUsers() ([]string, error) {
	var users []string
	return users, a.sendAny(http.MethodGet, usersEndpoint, nil, &users)
}
