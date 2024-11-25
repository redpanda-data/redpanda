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

	"connectrpc.com/connect"
)

// DataPlaneClientSet holds the respective service clients to interact with
// the data plane endpoints of the Public API.
type DataPlaneClientSet struct {
	Transform transformServiceClient
}

// NewDataPlaneClientSet creates a Public API client set with the service
// clients of each resource available to interact with this package.
func NewDataPlaneClientSet(host, authToken string, opts ...connect.ClientOption) (*DataPlaneClientSet, error) {
	if host == "" {
		return nil, fmt.Errorf("dataplane host is empty")
	}
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newAuthInterceptor(authToken), // Add the Bearer token.
			newLoggerInterceptor(),        // Add logs to every request.
		),
	}, opts...)

	return &DataPlaneClientSet{
		Transform: newTransformServiceClient(http.DefaultClient, host, authToken, opts...),
	}, nil
}
