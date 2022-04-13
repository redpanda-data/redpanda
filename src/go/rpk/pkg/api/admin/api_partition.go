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
	"fmt"
	"net/http"
)

// Replica contains the information of a partition replica
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

// GetPartition returns detailed partition information
func (a *AdminAPI) GetPartition(
	namespace, topic string, partition int,
) (Partition, error) {
	var pa Partition
	return pa, a.sendAny(
		http.MethodGet,
		fmt.Sprintf("/v1/partitions/%s/%s/%d", namespace, topic, partition),
		nil,
		&pa)
}

// UpdateReplicas updates replicas for a partition
func (a *AdminAPI) UpdateReplicas(
	namespace, topic string, partition int, sourceBroker int, targetBroker int,
) (Partition, error) {
	var pa Partition
        err := a.sendAny(
                http.MethodGet,
                fmt.Sprintf("/v1/partitions/%s/%s/%d", namespace, topic, partition),
                nil,
                &pa)
        if err != nil {
        	return pa, err
        }
        pa.Replicas = replaceNodeID(pa.Replicas, sourceBroker, targetBroker)
        err = a.sendAny(
                http.MethodPost,
                fmt.Sprintf("/v1/partitions/%s/%s/%d/replicas", namespace, topic, partition),
                pa.Replicas,
                &pa)
        return pa, err
}


func replaceNodeID(rs []Replica, sourceBroker int, targetBroker int) []Replica {
	var newReplicas []Replica
        for _, rep := range rs {        	
        	if rep.NodeID == sourceBroker {
        		var newRep Replica     		
        		newRep.NodeID = targetBroker
			newRep.Core = rep.Core
			newReplicas = append(newReplicas, newRep)
        	} else {
        		newReplicas = append(newReplicas, rep)
        	}
        }
	return newReplicas
}