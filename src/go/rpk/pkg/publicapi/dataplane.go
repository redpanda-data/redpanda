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
	"fmt"
	"net/http"

	"buf.build/gen/go/redpandadata/dataplane/connectrpc/go/redpanda/api/dataplane/v1/dataplanev1connect"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

// DataPlaneClientSet holds the respective service clients to interact with
// the data plane endpoints of the Public API.
type DataPlaneClientSet struct {
	Transform    transformServiceClient
	CloudStorage dataplanev1connect.CloudStorageServiceClient
	User         dataplanev1connect.UserServiceClient
	Secrets      dataplanev1connect.SecretServiceClient
}

// NewDataPlaneClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewDataPlaneClientSet(host, authToken string, opts ...connect.ClientOption) (*DataPlaneClientSet, error) {
	if host == "" {
		return nil, fmt.Errorf("dataplane host is empty")
	}
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newAuthInterceptor(authToken),              // Add the Bearer token.
			newLoggerInterceptor(),                     // Add logs to every request.
			newAgentInterceptor(defaultRpkUserAgent()), // Add the User-Agent.
		),
	}, opts...)

	return &DataPlaneClientSet{
		Transform:    newTransformServiceClient(http.DefaultClient, host, authToken, opts...),
		CloudStorage: dataplanev1connect.NewCloudStorageServiceClient(http.DefaultClient, host, opts...),
		User:         dataplanev1connect.NewUserServiceClient(http.DefaultClient, host, opts...),
		Secrets:      dataplanev1connect.NewSecretServiceClient(http.DefaultClient, host, opts...),
	}, nil
}

// DataplaneClientFromRpkProfile creates a DataPlaneClientSet with the
// information loaded in the profile. If the profile is not from cloud it will
// return an error.
func DataplaneClientFromRpkProfile(p *config.RpkProfile, opts ...connect.ClientOption) (*DataPlaneClientSet, error) {
	url, err := p.CloudCluster.CheckClusterURL()
	if err != nil {
		return nil, fmt.Errorf("unable to get cluster information from your profile: %v", err)
	}
	return NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken, opts...)
}
