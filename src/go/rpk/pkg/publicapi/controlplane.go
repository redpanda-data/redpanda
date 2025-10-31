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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/controlplane/v1/controlplanev1connect"
	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/controlplane/v1beta2/controlplanev1beta2connect"
	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/iam/v1beta2/iamv1beta2connect"
	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	controlplanev1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta2"
	iamv1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/iam/v1beta2"
	"connectrpc.com/connect"
)

// CloudClientSet holds the respective service clients to interact with
// the control plane endpoints of the Public API.
type CloudClientSet struct {
	Cluster       controlplanev1beta2connect.ClusterServiceClient
	Organization  iamv1beta2connect.OrganizationServiceClient
	ResourceGroup controlplanev1beta2connect.ResourceGroupServiceClient
	Serverless    controlplanev1beta2connect.ServerlessClusterServiceClient
	ServerlessV1  controlplanev1connect.ServerlessClusterServiceClient
}

// NewCloudClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewCloudClientSet(host, authToken string, opts ...connect.ClientOption) *CloudClientSet {
	if host == "" {
		host = ControlPlaneProdURL
	}
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newAuthInterceptor(authToken), // Add the Bearer token.
			newLoggerInterceptor(),        // Add logs to every request.
		),
	}, opts...)

	httpCl := &http.Client{Timeout: 30 * time.Second}

	return &CloudClientSet{
		Cluster:       controlplanev1beta2connect.NewClusterServiceClient(httpCl, host, opts...),
		Organization:  iamv1beta2connect.NewOrganizationServiceClient(httpCl, host, opts...),
		ResourceGroup: controlplanev1beta2connect.NewResourceGroupServiceClient(httpCl, host, opts...),
		Serverless:    controlplanev1beta2connect.NewServerlessClusterServiceClient(httpCl, host, opts...),
		ServerlessV1:  controlplanev1connect.NewServerlessClusterServiceClient(httpCl, host, opts...),
	}
}

// ResourceGroupForID gets the resource group for a given ID and handles the
// error if the returned resource group is nil.
func (cpCl *CloudClientSet) ResourceGroupForID(ctx context.Context, ID string) (*controlplanev1beta2.ResourceGroup, error) {
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

// ResourceGroups returns all the ResourceGroups using the pagination feature
// to traverse all pages of the list.
func (cpCl *CloudClientSet) ResourceGroups(ctx context.Context) ([]*controlplanev1beta2.ResourceGroup, error) {
	maxPages := 200
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1beta2.ResourceGroup, string, error) {
		req := connect.NewRequest(&controlplanev1beta2.ListResourceGroupsRequest{PageToken: pageToken, PageSize: 100})
		resp, err := cpCl.ResourceGroup.ListResourceGroups(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return resp.Msg.GetResourceGroups(), resp.Msg.GetNextPageToken(), nil
	}
	return Paginate(ctx, maxPages, fetchPage)
}

// ServerlessClusters returns all the ServerlessClusters using the pagination
// feature to traverse all pages of the list.
func (cpCl *CloudClientSet) ServerlessClusters(ctx context.Context) ([]*controlplanev1beta2.ServerlessCluster, error) {
	maxPages := 500
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1beta2.ServerlessCluster, string, error) {
		req := connect.NewRequest(&controlplanev1beta2.ListServerlessClustersRequest{PageToken: pageToken, PageSize: 100})
		resp, err := cpCl.Serverless.ListServerlessClusters(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return resp.Msg.ServerlessClusters, resp.Msg.NextPageToken, nil
	}
	return Paginate(ctx, maxPages, fetchPage)
}

func (cpCl *CloudClientSet) ServerlessClusterForID(ctx context.Context, ID string) (*controlplanev1beta2.ServerlessCluster, error) {
	c, err := cpCl.Serverless.GetServerlessCluster(ctx, connect.NewRequest(&controlplanev1beta2.GetServerlessClusterRequest{
		Id: ID,
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to request serverless cluster %q information: %w", ID, err)
	}
	if c.Msg.ServerlessCluster == nil {
		return nil, fmt.Errorf("unable to find serverless cluster %q; please report this bug to Redpanda Support", ID)
	}
	return c.Msg.ServerlessCluster, nil
}

func (cpCl *CloudClientSet) ServerlessClusterV1ForID(ctx context.Context, ID string) (*controlplanev1.ServerlessCluster, error) {
	c, err := cpCl.ServerlessV1.GetServerlessCluster(ctx, connect.NewRequest(&controlplanev1.GetServerlessClusterRequest{
		Id: ID,
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to request serverless cluster %q information: %w", ID, err)
	}
	if c.Msg.ServerlessCluster == nil {
		return nil, fmt.Errorf("unable to find serverless cluster %q; please report this bug to Redpanda Support", ID)
	}
	return c.Msg.ServerlessCluster, nil
}

// Clusters returns all the Clusters using the pagination feature to traverse
// all pages of the list.
func (cpCl *CloudClientSet) Clusters(ctx context.Context) ([]*controlplanev1beta2.Cluster, error) {
	maxPages := 500
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1beta2.Cluster, string, error) {
		req := connect.NewRequest(&controlplanev1beta2.ListClustersRequest{PageToken: pageToken, PageSize: 100})
		resp, err := cpCl.Cluster.ListClusters(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return resp.Msg.Clusters, resp.Msg.NextPageToken, nil
	}
	return Paginate(ctx, maxPages, fetchPage)
}

// ClusterForID gets the Cluster for a given ID and handles the error if the
// returned cluster is nil.
func (cpCl *CloudClientSet) ClusterForID(ctx context.Context, ID string) (*controlplanev1beta2.Cluster, error) {
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

// OrgResourceGroupsClusters is a helper function to concurrently query many
// APIs at once. Any non-nil error result is returned, as well as
// an errors.Joined error.
func (cpCl *CloudClientSet) OrgResourceGroupsClusters(ctx context.Context) (*iamv1beta2.Organization, []*controlplanev1beta2.ResourceGroup, []*controlplanev1beta2.ServerlessCluster, []*controlplanev1beta2.Cluster, error) {
	var (
		org    *iamv1beta2.Organization
		orgErr error
		rgs    []*controlplanev1beta2.ResourceGroup
		rgsErr error
		vcs    []*controlplanev1beta2.ServerlessCluster
		vcErr  error
		cs     []*controlplanev1beta2.Cluster
		cErr   error

		wg sync.WaitGroup
	)
	ctx, cancel := context.WithCancel(ctx)

	wg.Add(4)
	go func() {
		defer wg.Done()
		resp, err := cpCl.Organization.GetCurrentOrganization(ctx, connect.NewRequest(&iamv1beta2.GetCurrentOrganizationRequest{}))
		if err != nil {
			orgErr = fmt.Errorf("organization query failure: %w", err)
			cancel()
		} else {
			org = resp.Msg.GetOrganization()
		}
	}()
	go func() {
		defer wg.Done()
		rgs, rgsErr = cpCl.ResourceGroups(ctx)
		if rgsErr != nil {
			rgsErr = fmt.Errorf("resource group query failure: %w", rgsErr)
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		vcs, vcErr = cpCl.ServerlessClusters(ctx)
		if vcErr != nil {
			vcErr = fmt.Errorf("virtual Cluster query failure: %w", vcErr)
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		cs, cErr = cpCl.Clusters(ctx)
		if cErr != nil {
			cErr = fmt.Errorf("cluster query failure: %w", cErr)
			cancel()
		}
	}()
	wg.Wait()

	err := errors.Join(orgErr, rgsErr, vcErr, cErr)
	return org, rgs, vcs, cs, err
}

// Paginate abstracts pagination logic for API calls to the PublicAPI endpoints
// which have pagination enabled.
func Paginate[T any](
	ctx context.Context,
	maxPages int,
	fetchPage func(ctx context.Context, pageToken string) ([]T, string, error),
) ([]T, error) {
	var (
		pageToken string
		allItems  []T
	)
	for range maxPages {
		items, nextPageToken, err := fetchPage(ctx, pageToken)
		if err != nil {
			return nil, err
		}
		allItems = append(allItems, items...)
		if nextPageToken == "" {
			return allItems, nil
		}
		pageToken = nextPageToken
	}

	return nil, fmt.Errorf("pagination exceeded %d pages", maxPages)
}
