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
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/controlplane/v1beta1/controlplanev1beta1connect"
)

// ControlPlaneClientSet holds the respective service clients to interact with
// the control plane endpoints of the Public API.
type ControlPlaneClientSet struct {
	Namespace controlplanev1beta1connect.NamespaceServiceClient
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
		Namespace: controlplanev1beta1connect.NewNamespaceServiceClient(httpCl, host, opts...),
	}, nil
}
