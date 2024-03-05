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

	"connectrpc.com/connect"
	"go.uber.org/zap"
)

// ControlPlaneProdURL is the host of the Cloud Redpanda API.
const (
	ControlPlaneProdURL   = "https://api.redpanda.com"
	ServerlessClusterType = "TYPE_SERVERLESS"
)

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
