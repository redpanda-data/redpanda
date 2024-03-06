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

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/controlplane/v1beta1/controlplanev1beta1connect"
	"go.uber.org/zap"
)

// ProdURL is the host of the Cloud Redpanda API.
const ProdURL = "https://api.redpanda.com"

// ClientSet holds the respective service clients to interact with the Public
// API.
type ClientSet struct {
	Namespace controlplanev1beta1connect.NamespaceServiceClient
}

// NewClientSet creates a Public API client set with the service clients of
// each resource available to interact with this package.
func NewClientSet(host, authToken string, opts ...connect.ClientOption) (*ClientSet, error) {
	if host == "" {
		host = ProdURL
	}
	opts = append([]connect.ClientOption{
		connect.WithInterceptors(
			newAuthInterceptor(authToken), // Add the Bearer token.
			newLoggerInterceptor(),        // Add logs to every request.
		),
	}, opts...)

	httpCl := &http.Client{Timeout: 15 * time.Second}

	return &ClientSet{
		Namespace: controlplanev1beta1connect.NewNamespaceServiceClient(httpCl, host, opts...),
	}, nil
}

func newAuthInterceptor(token string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", fmt.Sprintf("Bearer %s", token))
			return next(ctx, req)
		}
	}
}

func newLoggerInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			zap.L().Debug("Sending request", zap.String("host", req.Peer().Addr), zap.String("procedure", req.Spec().Procedure))
			return next(ctx, req)
		}
	}
}
