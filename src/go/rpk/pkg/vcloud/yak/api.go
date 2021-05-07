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
	// GetClusters lists all redpanda clusters available in given namespace.
	// Returns `ErrLoginTokenMissing` if token cannot be retrieved. Returns
	// `ErrNotAuthorized` if user is not authorized to list clusters. Returns
	// `ErrNamespaceDoesNotExists` if namespace of given name was not
	// found.
	GetClusters(namespaceName string) ([]*Cluster, error)
}

type Namespace struct {
	Id         string   `json:"id,omitempty"`
	Name       string   `json:"name,omitempty"`
	ClusterIds []string `json:"clusterIds,omitempty"`
}

// Cluster is definition of redpanda cluster
type Cluster struct {
	Id       string   `json:"id,omitempty"`
	Name     string   `json:"name,omitempty"`
	Hosts    []string `json:"hosts,omitempty"`
	Ready    bool     `json:"ready,omitempty"`
	Provider string   `json:"provider,omitempty"`
	Region   string   `json:"region,omitempty"`
}

type ErrLoginTokenMissing struct {
	InnerError error
}

func (e ErrLoginTokenMissing) Error() string {
	return fmt.Sprintf("Error retrieving login token: %v", e.InnerError)
}

type ErrNamespaceDoesNotExist struct {
	Name string
}

func (e ErrNamespaceDoesNotExist) Error() string {
	return fmt.Sprintf("Namespace %s does not exist", e.Name)
}

var (
	ErrNotAuthorized = errors.New("User is not authorized to view this resource")
)
