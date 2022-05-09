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
	"net/http"
	"sort"
)

// ClusterView contains the broker list of a cluster and a cluster view version
// which it's monotonically increasing per each cluster view change.
type ClusterView struct {
	Version int      `json:"version"`
	Brokers []Broker `json:"brokers"`
}

// Health overview data structure.
type ClusterHealthOverview struct {
	IsHealthy            bool     `json:"is_healthy"`
	ControllerID         int      `json:"controller_id"`
	AllNodes             []int    `json:"all_nodes"`
	NodesDown            []int    `json:"nodes_down"`
	LeaderlessPartitions []string `json:"leaderless_partition"`
}

func (a *AdminAPI) GetHealthOverview() (ClusterHealthOverview, error) {
	var response ClusterHealthOverview
	return response, a.sendAny(http.MethodGet, "/v1/cluster/health_overview", nil, &response)
}

// ClusterView queries one of the client's hosts and returns the list of brokers
// and cluster view version.
func (a *AdminAPI) ClusterView() (ClusterView, error) {
	var cv ClusterView
	defer func() {
		sort.Slice(cv.Brokers, func(i, j int) bool { return cv.Brokers[i].NodeID < cv.Brokers[j].NodeID }) //nolint:revive // return inside this deferred function is for the sort's less function
	}()
	err := a.sendAny(http.MethodGet, "/v1/cluster_view", nil, &cv)
	return cv, err
}
