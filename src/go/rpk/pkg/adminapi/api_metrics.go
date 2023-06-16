// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package admin

import (
	"context"
	"net/http"
)

func (a *AdminAPI) PrometheusMetrics(ctx context.Context) ([]byte, error) {
	var res []byte
	return res, a.sendOne(ctx, http.MethodGet, "/metrics", nil, &res, false)
}

func (a *AdminAPI) PublicMetrics(ctx context.Context) ([]byte, error) {
	var res []byte
	return res, a.sendOne(ctx, http.MethodGet, "/public_metrics", nil, &res, false)
}
