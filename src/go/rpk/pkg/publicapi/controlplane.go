// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package publicapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	controlplanev1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta2"

	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/controlplane/v1beta2/controlplanev1beta2connect"
	"connectrpc.com/connect"
)

// ControlPlaneClientSet holds the respective service clients to interact with
// the control plane endpoints of the Public API.
type ControlPlaneClientSet struct {
	ResourceGroup controlplanev1beta2connect.ResourceGroupServiceClient
	Cluster       controlplanev1beta2connect.ClusterServiceClient
}

// NewControlPlaneClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewControlPlaneClientSet(host, authToken string, opts ...connect.ClientOption) (*ControlPlaneClientSet, error) {
	if host == "" {
		host = ControlPlaneProdURL
	}
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newAuthInterceptor(authToken), // Add the Bearer token.
			newLoggerInterceptor(),        // Add logs to every request.
		),
	}, opts...)

	httpCl := &http.Client{Timeout: 15 * time.Second}

	return &ControlPlaneClientSet{
		ResourceGroup: controlplanev1beta2connect.NewResourceGroupServiceClient(httpCl, host, opts...),
		Cluster:       controlplanev1beta2connect.NewClusterServiceClient(httpCl, host, opts...),
	}, nil
}

// ResourceGroupForID gets the resource group for a given ID and handles the
// error if the returned resource group is nil.
func (cpCl *ControlPlaneClientSet) ResourceGroupForID(ctx context.Context, ID string) (*controlplanev1beta2.ResourceGroup, error) {
	rg, err := cpCl.ResourceGroup.GetResourceGroup(ctx, connect.NewRequest(&controlplanev1beta2.GetResourceGroupRequest{
		Id: ID,
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to request resource group with ID %q: %w", ID, err)
	}
	if rg.Msg.ResourceGroup == nil {
		// This should not happen but the new API returns a pointer, and we
		// need to make sure that a ResourceGroup is returned
		return nil, fmt.Errorf("unable to request resource group with ID %q: resource group does not exist; please report this with Redpanda Support", ID)
	}
	return rg.Msg.ResourceGroup, nil
}

// ClusterForID gets the Cluster for a given ID and handles the error if the
// returned cluster is nil.
func (cpCl *ControlPlaneClientSet) ClusterForID(ctx context.Context, ID string) (*controlplanev1beta2.Cluster, error) {
	c, err := cpCl.Cluster.GetCluster(ctx, connect.NewRequest(&controlplanev1beta2.GetClusterRequest{
		Id: ID,
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to request cluster %q information: %w", ID, err)
	}
	if c.Msg.Cluster == nil {
		return nil, fmt.Errorf("unable to find cluster %q; please report this bug to Redpanda Support", ID)
	}
	return c.Msg.Cluster, nil
}
