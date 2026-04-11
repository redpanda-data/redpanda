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

	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/byocplugin/v1alpha1/byocpluginv1alpha1connect"
	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/controlplane/v1/controlplanev1connect"
	"buf.build/gen/go/redpandadata/cloud/connectrpc/go/redpanda/api/iam/v1/iamv1connect"
	byocpluginv1alpha1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/byocplugin/v1alpha1"
	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	iamv1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/iam/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/authtoken"
)

// CloudClientSet holds the respective service clients to interact with
// the control plane endpoints of the Public API.
type CloudClientSet struct {
	// Controlplane
	Region           controlplanev1connect.RegionServiceClient
	Cluster          controlplanev1connect.ClusterServiceClient
	Network          controlplanev1connect.NetworkServiceClient
	ResourceGroup    controlplanev1connect.ResourceGroupServiceClient
	Serverless       controlplanev1connect.ServerlessClusterServiceClient
	Operations       controlplanev1connect.OperationServiceClient
	ServerlessRegion controlplanev1connect.ServerlessRegionServiceClient
	BYOCPlugin       byocpluginv1alpha1connect.BYOCPluginServiceClient
	ShadowLink       controlplanev1connect.ShadowLinkServiceClient

	// IAM
	Organization   iamv1connect.OrganizationServiceClient
	Permission     iamv1connect.PermissionServiceClient
	Role           iamv1connect.RoleServiceClient
	RoleBinding    iamv1connect.RoleBindingServiceClient
	ServiceAccount iamv1connect.ServiceAccountServiceClient
	IAMUser        iamv1connect.UserServiceClient
	UserInvite     iamv1connect.UserInviteServiceClient

	m         sync.RWMutex
	authToken string
}

func (cpCl *CloudClientSet) Token() string {
	cpCl.m.RLock()
	defer cpCl.m.RUnlock()
	return cpCl.authToken
}

// NewCloudClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewCloudClientSet(host, authToken string, opts ...connect.ClientOption) *CloudClientSet {
	if host == "" {
		host = ControlPlaneProdURL
	}
	ccs := &CloudClientSet{}
	ccs.authToken = authToken
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newReloadingAuthInterceptor(ccs.Token),     // Add the Bearer token.
			newLoggerInterceptor(),                     // Add logs to every request.
			newAgentInterceptor(defaultRpkUserAgent()), // Add the User-Agent.
		),
	}, opts...)

	httpCl := &http.Client{Timeout: 30 * time.Second}

	ccs.Cluster = controlplanev1connect.NewClusterServiceClient(httpCl, host, opts...)
	ccs.Region = controlplanev1connect.NewRegionServiceClient(httpCl, host, opts...)
	ccs.Network = controlplanev1connect.NewNetworkServiceClient(httpCl, host, opts...)
	ccs.ResourceGroup = controlplanev1connect.NewResourceGroupServiceClient(httpCl, host, opts...)
	ccs.Serverless = controlplanev1connect.NewServerlessClusterServiceClient(httpCl, host, opts...)
	ccs.Operations = controlplanev1connect.NewOperationServiceClient(httpCl, host, opts...)
	ccs.ServerlessRegion = controlplanev1connect.NewServerlessRegionServiceClient(httpCl, host, opts...)
	ccs.BYOCPlugin = byocpluginv1alpha1connect.NewBYOCPluginServiceClient(httpCl, host, opts...)
	ccs.ShadowLink = controlplanev1connect.NewShadowLinkServiceClient(httpCl, host, opts...)

	ccs.Organization = iamv1connect.NewOrganizationServiceClient(httpCl, host, opts...)
	ccs.Permission = iamv1connect.NewPermissionServiceClient(httpCl, host, opts...)
	ccs.Role = iamv1connect.NewRoleServiceClient(httpCl, host, opts...)
	ccs.RoleBinding = iamv1connect.NewRoleBindingServiceClient(httpCl, host, opts...)
	ccs.ServiceAccount = iamv1connect.NewServiceAccountServiceClient(httpCl, host, opts...)
	ccs.IAMUser = iamv1connect.NewUserServiceClient(httpCl, host, opts...)
	ccs.UserInvite = iamv1connect.NewUserInviteServiceClient(httpCl, host, opts...)
	return ccs
}

// NewValidatedCloudClientSet creates a Public API client set after validating
// the provided auth token against the given audience and client IDs.
func NewValidatedCloudClientSet(host, authToken, audience string, clientIDs []string, opts ...connect.ClientOption) (*CloudClientSet, error) {
	err := authtoken.ValidateTokenOrExpire(authToken, audience, clientIDs...)
	if err != nil {
		return nil, fmt.Errorf("invalid Redpanda Cloud token: %v", err)
	}

	return NewCloudClientSet(host, authToken, opts...), nil
}

func (cpCl *CloudClientSet) UpdateAuthToken(authToken string) {
	cpCl.m.Lock()
	defer cpCl.m.Unlock()
	cpCl.authToken = authToken
}

// ResourceGroupForID gets the resource group for a given ID and handles the
// error if the returned resource group is nil.
func (cpCl *CloudClientSet) ResourceGroupForID(ctx context.Context, ID string) (*controlplanev1.ResourceGroup, error) {
	rg, err := cpCl.ResourceGroup.GetResourceGroup(ctx, connect.NewRequest(&controlplanev1.GetResourceGroupRequest{
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
func (cpCl *CloudClientSet) ResourceGroups(ctx context.Context) ([]*controlplanev1.ResourceGroup, error) {
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1.ResourceGroup, string, error) {
		req := connect.NewRequest(&controlplanev1.ListResourceGroupsRequest{PageToken: pageToken, PageSize: 100})
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
func (cpCl *CloudClientSet) ServerlessClusters(ctx context.Context) ([]*controlplanev1.ServerlessCluster, error) {
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1.ServerlessCluster, string, error) {
		req := connect.NewRequest(&controlplanev1.ListServerlessClustersRequest{PageToken: pageToken, PageSize: 100})
		resp, err := cpCl.Serverless.ListServerlessClusters(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return resp.Msg.ServerlessClusters, resp.Msg.NextPageToken, nil
	}
	return Paginate(ctx, maxPages, fetchPage)
}

func (cpCl *CloudClientSet) ServerlessClusterForID(ctx context.Context, ID string) (*controlplanev1.ServerlessCluster, error) {
	c, err := cpCl.Serverless.GetServerlessCluster(ctx, connect.NewRequest(&controlplanev1.GetServerlessClusterRequest{
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
func (cpCl *CloudClientSet) Clusters(ctx context.Context) ([]*controlplanev1.Cluster, error) {
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1.Cluster, string, error) {
		req := connect.NewRequest(&controlplanev1.ListClustersRequest{PageToken: pageToken, PageSize: 100})
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
func (cpCl *CloudClientSet) ClusterForID(ctx context.Context, ID string) (*controlplanev1.Cluster, error) {
	c, err := cpCl.Cluster.GetCluster(ctx, connect.NewRequest(&controlplanev1.GetClusterRequest{
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

// ShadowLinkListItems returns all the ShadowLinkListItems using the pagination
// feature to traverse all pages of the list.
func (cpCl *CloudClientSet) ShadowLinkListItems(ctx context.Context, filter *controlplanev1.ListShadowLinksRequest_Filter) ([]*controlplanev1.ShadowLinkListItem, error) {
	fetchPage := func(ctx context.Context, pageToken string) ([]*controlplanev1.ShadowLinkListItem, string, error) {
		req := connect.NewRequest(&controlplanev1.ListShadowLinksRequest{
			Filter:    filter,
			PageToken: pageToken,
			PageSize:  100,
		})
		resp, err := cpCl.ShadowLink.ListShadowLinks(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return resp.Msg.ShadowLinks, resp.Msg.NextPageToken, nil
	}
	return Paginate(ctx, maxPages, fetchPage)
}

func (cpCl *CloudClientSet) ShadowLinkByNameAndRPID(ctx context.Context, name, redpandaID string) (*controlplanev1.ShadowLink, error) {
	list, err := cpCl.ShadowLinkListItems(ctx, &controlplanev1.ListShadowLinksRequest_Filter{
		ShadowRedpandaId: redpandaID,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list Shadow Links for cluster with ID %q: %w", redpandaID, err)
	}
	var foundSLID string
	for _, l := range list {
		if l.GetName() == name {
			foundSLID = l.GetId()
			break
		}
	}
	if foundSLID == "" {
		return nil, fmt.Errorf("unable to find Shadow Link %q in the cluster with ID %q", name, redpandaID)
	}
	link, err := cpCl.ShadowLink.GetShadowLink(ctx, connect.NewRequest(&controlplanev1.GetShadowLinkRequest{
		Id: foundSLID,
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to get Shadow Link %q for cluster with ID %q: %w", name, redpandaID, err)
	}
	return link.Msg.ShadowLink, nil
}

// OrgResourceGroupsClusters is a helper function to concurrently query many
// APIs at once. Any non-nil error result is returned, as well as
// an errors.Joined error.
func (cpCl *CloudClientSet) OrgResourceGroupsClusters(ctx context.Context) (*iamv1.Organization, []*controlplanev1.ResourceGroup, []*controlplanev1.ServerlessCluster, []*controlplanev1.Cluster, error) {
	var (
		org    *iamv1.Organization
		orgErr error
		rgs    []*controlplanev1.ResourceGroup
		rgsErr error
		vcs    []*controlplanev1.ServerlessCluster
		vcErr  error
		cs     []*controlplanev1.Cluster
		cErr   error

		wg sync.WaitGroup
	)
	ctx, cancel := context.WithCancel(ctx)

	wg.Add(4)
	go func() {
		defer wg.Done()
		resp, err := cpCl.Organization.GetCurrentOrganization(ctx, connect.NewRequest(&iamv1.GetCurrentOrganizationRequest{}))
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

func OSToBYOCPluginOS(os string) byocpluginv1alpha1.OS {
	switch os {
	case "linux":
		return byocpluginv1alpha1.OS_OS_LINUX
	case "darwin":
		return byocpluginv1alpha1.OS_OS_DARWIN
	case "windows":
		return byocpluginv1alpha1.OS_OS_WINDOWS
	default:
		return byocpluginv1alpha1.OS_OS_UNSPECIFIED
	}
}

func ArchToBYOCPluginArch(arch string) byocpluginv1alpha1.Arch {
	switch arch {
	case "amd64":
		return byocpluginv1alpha1.Arch_ARCH_AMD64
	case "arm64":
		return byocpluginv1alpha1.Arch_ARCH_ARM64
	default:
		return byocpluginv1alpha1.Arch_ARCH_UNSPECIFIED
	}
}
