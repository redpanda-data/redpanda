// Copyright 2021 Redpanda Data, Inc.
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
	"fmt"
	"io"
	"net/http"
	"strings"
)

const partitionsBaseURL = "/v1/cluster/partitions"

// Replica contains the information of a partition replica.
type Replica struct {
	NodeID int `json:"node_id"  yaml:"node_id"`
	Core   int `json:"core" yaml:"core"`
}

type Replicas []Replica

func (rs Replicas) String() string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, r := range rs {
		if i > 0 {
			io.WriteString(&sb, ", ")
		}
		fmt.Fprintf(&sb, "%d-%d", r.NodeID, r.Core)
	}
	sb.WriteByte(']')
	return sb.String()
}

// Partition is the information returned from the Redpanda admin partitions endpoints.
type Partition struct {
	Namespace   string    `json:"ns"`
	Topic       string    `json:"topic"`
	PartitionID int       `json:"partition_id"`
	Status      string    `json:"status"`
	LeaderID    int       `json:"leader_id"`
	RaftGroupID int       `json:"raft_group_id"`
	Replicas    []Replica `json:"replicas"`
}

type Operation struct {
	Core        int    `json:"core"`
	RetryNumber int    `json:"retry_number"`
	Revision    int    `json:"revision"`
	Status      string `json:"status"`
	Type        string `json:"type"`
}

type Status struct {
	NodeID     int         `json:"node_id"`
	Operations []Operation `json:"operations"`
}

// Reconfiguration is the detail of a partition reconfiguration.
type ReconfigurationsResponse struct {
	Ns                     string    `json:"ns"`
	Topic                  string    `json:"topic"`
	PartitionID            int       `json:"partition"`
	PartitionSize          int       `json:"partition_size"`
	BytesMoved             int       `json:"bytes_moved"`
	BytesLeft              int       `json:"bytes_left_to_move"`
	PreviousReplicas       []Replica `json:"previous_replicas"`
	NewReplicas            []Replica `json:"current_replicas"`
	ReconciliationStatuses []Status  `json:"reconciliation_statuses"`
}

type ClusterPartition struct {
	Ns          string    `json:"ns" yaml:"ns"`
	Topic       string    `json:"topic" yaml:"topic"`
	PartitionID int       `json:"partition_id" yaml:"partition_id"`
	LeaderID    *int      `json:"leader_id,omitempty" yaml:"leader_id,omitempty"` // LeaderID may be missing in the response.
	Replicas    []Replica `json:"replicas" yaml:"replicas"`
	Disabled    *bool     `json:"disabled,omitempty" yaml:"disabled,omitempty"` // Disabled may be discarded if not present.
}

// GetPartition returns detailed partition information.
func (a *AdminAPI) GetPartition(
	ctx context.Context, namespace, topic string, partition int,
) (Partition, error) {
	var pa Partition
	return pa, a.sendAny(ctx, http.MethodGet, fmt.Sprintf("/v1/partitions/%s/%s/%d", namespace, topic, partition), nil, &pa)
}

// GetTopic returns detailed information of all partitions for a given topic.
func (a *AdminAPI) GetTopic(ctx context.Context, namespace, topic string) ([]Partition, error) {
	var pa []Partition
	return pa, a.sendAny(ctx, http.MethodGet, fmt.Sprintf("/v1/partitions/%s/%s", namespace, topic), nil, &pa)
}

// Reconfigurations returns the list of ongoing partition reconfigurations.
func (a *AdminAPI) Reconfigurations(ctx context.Context) ([]ReconfigurationsResponse, error) {
	var rr []ReconfigurationsResponse
	return rr, a.sendAny(ctx, http.MethodGet, "/v1/partitions/reconfigurations", nil, &rr)
}

// AllClusterPartitions returns cluster level metadata of all partitions in a
// cluster. If withInternal is true, internal topics will be returned. If
// disabled is true, only disabled partitions are returned.
func (a *AdminAPI) AllClusterPartitions(ctx context.Context, withInternal, disabled bool) ([]ClusterPartition, error) {
	var clusterPartitions []ClusterPartition
	partitionsURL := fmt.Sprintf("%v?with_internal=%v&disabled=%v", partitionsBaseURL, withInternal, disabled)
	return clusterPartitions, a.sendAny(ctx, http.MethodGet, partitionsURL, nil, &clusterPartitions)
}

// TopicClusterPartitions returns cluster level metadata of all partitions in
// a given topic. If disabled is true, only disabled partitions are returned.
func (a *AdminAPI) TopicClusterPartitions(ctx context.Context, namespace, topic string, disabled bool) ([]ClusterPartition, error) {
	var clusterPartition []ClusterPartition
	partitionURL := fmt.Sprintf("%v/%v/%v?disabled=%v", partitionsBaseURL, namespace, topic, disabled)
	return clusterPartition, a.sendAny(ctx, http.MethodGet, partitionURL, nil, &clusterPartition)
}

// MoveReplicas changes replica and core (aka shard) assignments for a given partition.
func (a *AdminAPI) MoveReplicas(ctx context.Context, ns string, topic string, part int, r []Replica) error {
	return a.sendToLeader(ctx,
		http.MethodPost,
		fmt.Sprintf("/v1/partitions/%s/%s/%d/replicas", ns, topic, part),
		r,
		nil)
}
