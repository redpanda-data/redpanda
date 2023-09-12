// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

import (
	"context"
	"net/http"
)

const (
	baseTransformEndpoint = "/v1/transform/"
	deleteSuffix          = "delete"
)

type transformDeleteRequest struct {
	Name string `json:"name"`
}

// Delete a wasm transform in a cluster.
func (a *AdminAPI) DeleteWasmTransform(ctx context.Context, name string) error {
	return a.sendToLeader(ctx, http.MethodPost, baseTransformEndpoint+deleteSuffix, transformDeleteRequest{name}, nil)
}
