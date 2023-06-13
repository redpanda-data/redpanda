// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloudapi

import (
	"context"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

const (
	virtualClusterPath = "/api/v1/virtual-clusters"
	ClusterStateReady  = "ready"
)

type (
	VirtualCluster struct {
		NameID
		NamespaceUUID string    `json:"namespaceUuid"`
		CreatedAt     time.Time `json:"createdAt"`
		State         string    `json:"state"`

		Status struct {
			Listeners struct {
				ConsoleURL    string   `json:"consoleUrl"`
				SeedAddresses []string `json:"seedAddress"`
			} `json:"listeners"`
		} `json:"status"`
	}
	// VirtualClusters is a set of Redpanda virtual clusters.
	VirtualClusters []VirtualCluster
)

func (cl *Client) VirtualClusters(ctx context.Context) (VirtualClusters, error) {
	var cs []VirtualCluster
	err := cl.cl.Get(ctx, virtualClusterPath, nil, &cs)
	return cs, err
}

func (cl *Client) VirtualCluster(ctx context.Context, vClusterID string) (VirtualCluster, error) {
	path := httpapi.Pathfmt(virtualClusterPath+"/%s", vClusterID)
	var vc VirtualCluster
	err := cl.cl.Get(ctx, path, nil, &vc)
	return vc, err
}
