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

const clusterPath = "/api/v1/clusters"

type (
	// ClusterSpec contains the current spec for a cluster.
	ClusterSpec struct {
		ClusterType        string   `json:"clusterType"`
		InstallPackVersion string   `json:"installPackVersion"`
		Provider           string   `json:"provider"`
		Region             string   `json:"region"`
		Zones              []string `json:"zones"`
		NodesCount         int      `json:"nodesCount"`
		MachineType        string   `json:"machineType"`

		KafkaListeners struct {
			Listeners []struct {
				SASL *struct {
					Protocol string `json:"protocol"`
				} `json:"sasl"`
				TLS *struct {
					RequireClientAuth bool `json:"requireClientAuth"` // if true, requires mTLS
				} `json:"tls"`
			} `json:"listeners"`
		} `json:"kafkaListeners"`
	}

	// Cluster contains information about a Redpanda cluster.
	Cluster struct {
		NameID
		NamespaceUUID string    `json:"namespaceUuid"`
		CreatedAt     time.Time `json:"createdAt"`
		State         string    `json:"state"`

		Spec   ClusterSpec `json:"spec"`
		Status struct {
			Health    string `json:"health"`
			Listeners struct {
				Kafka struct {
					Default struct {
						URLs []string `json:"urls"`
					} `json:"default"`
				} `json:"kafka"`
				Console struct {
					Default struct {
						URLs []string `json:"urls"`
					} `json:"default"`
				} `json:"redpandaConsole"`
			} `json:"listeners"`
		} `json:"status"`
	}

	// Clusters is a set of Redpanda clusters.
	Clusters []Cluster
)

// FilterNamespaceUUID returns all clusters that belong to the given namespace
// UUID.
func (cs Clusters) FilterNamespaceUUID(uuid string) Clusters {
	var ret Clusters
	for _, c := range cs {
		if c.NamespaceUUID == uuid {
			ret = append(ret, c)
		}
	}
	return ret
}

// Clusters returns the list of clusters in a Redpanda org.
func (cl *Client) Clusters(ctx context.Context) (Clusters, error) {
	var cs []Cluster
	err := cl.cl.Get(ctx, clusterPath, nil, &cs)
	return cs, err
}

// Cluster returns information about a Redpanda cluster.
func (cl *Client) Cluster(ctx context.Context, clusterID string) (Cluster, error) {
	path := httpapi.Pathfmt(clusterPath+"/%s", clusterID)
	var c Cluster
	err := cl.cl.Get(ctx, path, nil, &c)
	return c, err
}
