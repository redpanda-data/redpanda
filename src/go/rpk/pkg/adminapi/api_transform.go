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

// PartitionTransformStatus is the status of a single transform that is running on an input partition.
type PartitionTransformStatus struct {
	NodeID    int `json:"node_id"`
	Partition int `json:"partition"`
	Core      int `json:"core"`
	// Status is an enum of: ["running", "inactive", "errored", "unknown"].
	Status string `json:"status"`
}

// EnvironmentVariable is a configuration key/value that can be injected into to a data transform.
type EnvironmentVariable struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// TransformMetadata is the metadata for a live running transform on a cluster.
type TransformMetadata struct {
	Name         string                     `json:"name"`
	InputTopic   string                     `json:"input_topic"`
	OutputTopics []string                   `json:"output_topics"`
	Status       []PartitionTransformStatus `json:"status"`
	Environment  []EnvironmentVariable      `json:"environment"`
}

// ListWasmTransforms lists the transforms that are running on a cluster.
func (a *AdminAPI) ListWasmTransforms(ctx context.Context) ([]TransformMetadata, error) {
	resp := []TransformMetadata{}
	err := a.sendAny(ctx, http.MethodGet, baseTransformEndpoint, nil, &resp)
	return resp, err
}
