// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package yak

import (
	"errors"
	"fmt"
)

type CloudApiClient interface {
	// GetNamespaces returns list of all namespaces available in the user's
	// organization. Returns `ErrLoginTokenMissing` if token cannot be
	// retrieved. Returns `ErrNotAuthorized` if user is not authorized to list
	// namespaces.
	GetNamespaces() ([]*Namespace, error)
}

type Namespace struct {
	Id         string   `json:"id,omitempty"`
	Name       string   `json:"name,omitempty"`
	ClusterIds []string `json:"clusterIds,omitempty"`
}

type ErrLoginTokenMissing struct {
	InnerError error
}

func (e ErrLoginTokenMissing) Error() string {
	return fmt.Sprintf("Error retrieving login token: %v", e.InnerError)
}

var (
	ErrNotAuthorized = errors.New("User is not authorized to view this resource")
)
