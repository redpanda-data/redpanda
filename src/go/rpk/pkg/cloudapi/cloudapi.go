// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package cloudapi provides a client to talk to the Redpanda Cloud API.
package cloudapi

import (
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

// Client talks to the cloud API.
type Client struct {
	cl *httpapi.Client
}

// NewClient initializes and returns a client for talking to the cloud API.
// If the host is empty, this defaults to the prod API host.
func NewClient(host, authToken string) *Client {
	if host == "" {
		host = "https://cloud-api.prd.cloud.redpanda.com"
	}
	return &Client{
		cl: httpapi.NewClient(
			httpapi.Host(host),
			httpapi.BearerAuth(authToken),
		),
	}
}

// NameID is a common type used in may endpoints / many structs.
type NameID struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// Less returns whether this name is less than the other name.
func (n NameID) Less(other NameID) bool {
	return n.Name < other.Name || n.Name == other.Name && n.ID < other.ID
}

// NameIDs is an alias for a slice of names; this exists to provide
// an easy sorting method.
type NameIDs []NameID

// Sort allows for sorting before returning from API requests.
func (nids NameIDs) Sort() { sort.Slice(nids, func(i, j int) bool { return nids[i].Less(nids[j]) }) }
