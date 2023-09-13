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
	"net/http"
)

// Replica contains the information of a partition replica.
type Replica struct {
	NodeID int `json:"node_id"`
	Core   int `json:"core"`
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

// GetPartition returns detailed partition information.
func (a *AdminAPI) GetPartition(
	ctx context.Context, namespace, topic string, partition int,
) (Partition, error) {
	var pa Partition
	return pa, a.sendAny(
		ctx,
		http.MethodGet,
		fmt.Sprintf("/v1/partitions/%s/%s/%d", namespace, topic, partition),
		nil,
		&pa)
}

// Reconfigurations returns the list of ongoing partition reconfigurations.
func (a *AdminAPI) Reconfigurations(ctx context.Context) ([]ReconfigurationsResponse, error) {
	var rr []ReconfigurationsResponse
	return rr, a.sendAny(ctx, http.MethodGet, "/v1/partitions/reconfigurations", nil, &rr)
}
