// Copyright 2022 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
)

const clusterPath = "/api/v1/clusters"

// ClusterSpec contains the current spec for a cluster.
type ClusterSpec struct {
	InstallPackVersion string `json:"installPackVersion"`
}

// Cluster contains information about a Redpanda cluster.
type Cluster struct {
	NameID

	Spec ClusterSpec `json:"spec"`
}

// Cluster returns information about a Redpanda cluster.
func (cl *Client) Cluster(ctx context.Context, clusterID string) (c Cluster, err error) {
	path := httpapi.Pathfmt(clusterPath+"/%s", clusterID)
	return c, cl.cl.Get(ctx, path, nil, &c)
}
