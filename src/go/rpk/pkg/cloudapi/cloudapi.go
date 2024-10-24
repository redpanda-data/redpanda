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
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

// ProdURL is the hostname of the Redpanda API.
const ProdURL = "https://cloud-api.prd.cloud.redpanda.com"

// Client talks to the cloud API.
type Client struct {
	cl *httpapi.Client
}

// NewClient initializes and returns a client for talking to the cloud API.
// If the host is empty, this defaults to the prod API host.
func NewClient(host, authToken string, hopts ...httpapi.Opt) *Client {
	if host == "" {
		host = ProdURL
	}
	opts := []httpapi.Opt{
		httpapi.Host(host),
		httpapi.BearerAuth(authToken),
	}
	opts = append(opts, hopts...)
	return &Client{cl: httpapi.NewClient(opts...)}
}

// NameID is a common type used in many endpoints / many structs.
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

// OrgNamespacesClusters is a helper function to concurrently query many APIs
// at once. Any non-nil error result is returned, as well as an errors.Joined
// error.
func (cl *Client) OrgNamespacesClusters(ctx context.Context) (Organization, Namespaces, VirtualClusters, Clusters, error) {
	var (
		org    Organization
		orgErr error
		nss    Namespaces
		nsErr  error
		vcs    VirtualClusters
		vcErr  error
		cs     Clusters
		cErr   error

		wg sync.WaitGroup
	)
	ctx, cancel := context.WithCancel(ctx)

	wg.Add(4)
	go func() {
		defer wg.Done()
		org, orgErr = cl.Organization(ctx)
		if orgErr != nil {
			orgErr = fmt.Errorf("organization query failure: %w", orgErr)
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		nss, nsErr = cl.Namespaces(ctx)
		if nsErr != nil {
			nsErr = fmt.Errorf("namespace query failure: %w", nsErr)
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		vcs, vcErr = cl.VirtualClusters(ctx)
		if vcErr != nil {
			vcErr = fmt.Errorf("virtual cluster query failure: %w", vcErr)
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		cs, cErr = cl.Clusters(ctx)
		if cErr != nil {
			cErr = fmt.Errorf("cluster query failure: %w", cErr)
			cancel()
		}
	}()
	wg.Wait()

	err := errors.Join(orgErr, nsErr, vcErr, cErr)
	return org, nss, vcs, cs, err
}

// NamespacedCluster ties a cluster or vcluster to its namespace.
type NamespacedCluster struct {
	Namespace  Namespace
	Cluster    Cluster
	VCluster   VirtualCluster
	IsVCluster bool
}
