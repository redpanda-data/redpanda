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
