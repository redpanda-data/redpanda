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
	"crypto/tls"
	"fmt"

	controlplanev1beta1 "github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/controlplane/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// ProdURL is the host of the Cloud Redpanda API.
const ProdURL = "api.redpanda.com:443"

// ClientSet holds the respective service clients to interact with the Public
// API.
type ClientSet struct {
	Namespace controlplanev1beta1.NamespaceServiceClient
}

// NewClientSet creates a Public API client set with the service clients of
// each resource available to interact with this package.
func NewClientSet(ctx context.Context, host, authToken string, opts ...grpc.DialOption) (*ClientSet, error) {
	if host == "" {
		host = ProdURL
	}
	opts = append([]grpc.DialOption{
		// Intercept to add the Bearer token.
		grpc.WithUnaryInterceptor(
			func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoker(metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", authToken)), method, req, reply, cc, opts...)
			},
		),
		// Provide TLS config.
		grpc.WithTransportCredentials(
			credentials.NewTLS(
				&tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			),
		),
	}, opts...)

	conn, err := clientConnection(ctx, host, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to create a connection with %q", host)
	}
	return &ClientSet{
		Namespace: controlplanev1beta1.NewNamespaceServiceClient(conn),
	}, nil
}

func clientConnection(ctx context.Context, url string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.DialContext(
		ctx,
		url,
		opts...,
	)
}
