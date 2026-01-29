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
	"runtime"

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
	"go.uber.org/zap"
)

// ControlPlaneProdURL is the host of the Cloud Redpanda API.
const (
	ControlPlaneProdURL = "https://api.redpanda.com"
	maxPages            = 500 // maximum number of pages to fetch in paginated requests.
)

func newAuthInterceptor(token string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", fmt.Sprintf("Bearer %s", token))
			return next(ctx, req)
		}
	}
}

func newReloadingAuthInterceptor(f func() string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", fmt.Sprintf("Bearer %s", f()))
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

func defaultRpkUserAgent() string {
	return fmt.Sprintf("rpk/%s (%s_%s)", version.Version, runtime.GOOS, runtime.GOARCH)
}

func newAgentInterceptor(agent string) connect.Interceptor {
	return &agentInterceptor{agent: agent}
}

type agentInterceptor struct {
	agent string
}

func (i *agentInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("User-Agent", i.agent)
		return next(ctx, req)
	}
}

func (i *agentInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("User-Agent", i.agent)
		return conn
	}
}

func (*agentInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
