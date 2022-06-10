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

// Health overview data structure.
type ClusterHealthOverview struct {
	IsHealthy            bool     `json:"is_healthy"`
	ControllerID         int      `json:"controller_id"`
	AllNodes             []int    `json:"all_nodes"`
	NodesDown            []int    `json:"nodes_down"`
	LeaderlessPartitions []string `json:"leaderless_partition"`
}

func (a *AdminAPI) GetHealthOverview(ctx context.Context) (ClusterHealthOverview, error) {
	var response ClusterHealthOverview
	return response, a.sendAny(ctx, http.MethodGet, "/v1/cluster/health_overview", nil, &response)
}
