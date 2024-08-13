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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
)

const partitionsBaseURL = "/v1/cluster/partitions"

// Replica contains the information of a partition replica.
type Replica struct {
	NodeID int `json:"node_id"  yaml:"node_id"`
	Core   int `json:"core" yaml:"core"`
}

// NTP represents a partition's namespace, topic, and partition ID.
type NTP struct {
	Ns          string `json:"ns" yaml:"ns"`
	Topic       string `json:"topic" yaml:"topic"`
	PartitionID int    `json:"partition" yaml:"partition"`
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

// ReconfigurationsResponse is the detail of a partition reconfiguration.
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

type MajorityLostPartitions struct {
	NTP           NTP       `json:"ntp,omitempty" yaml:"ntp,omitempty"`
	TopicRevision int       `json:"topic_revision" yaml:"topic_revision"`
	Replicas      []Replica `json:"replicas,omitempty" yaml:"replicas,omitempty"`
	DeadNodes     []int     `json:"dead_nodes,omitempty" yaml:"dead_nodes,omitempty"`
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
	partitionsURL := fmt.Sprintf("%v?with_internal=%v", partitionsBaseURL, withInternal)
	if disabled {
		partitionsURL += "&disabled=true"
	}
	return clusterPartitions, a.sendAny(ctx, http.MethodGet, partitionsURL, nil, &clusterPartitions)
}

// TopicClusterPartitions returns cluster level metadata of all partitions in
// a given topic. If disabled is true, only disabled partitions are returned.
func (a *AdminAPI) TopicClusterPartitions(ctx context.Context, namespace, topic string, disabled bool) ([]ClusterPartition, error) {
	var clusterPartition []ClusterPartition
	partitionURL := fmt.Sprintf("%v/%v/%v", partitionsBaseURL, namespace, topic)
	if disabled {
		partitionURL += "?disabled=true"
	}
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

// ToggleAllTopicPartitions will toggle all partitions in the given topic.
func (a *AdminAPI) ToggleAllTopicPartitions(ctx context.Context, disabled bool, namespace, topic string) error {
	disableURL := fmt.Sprintf("%v/%v/%v", partitionsBaseURL, namespace, topic)
	body := struct {
		Disabled bool `json:"disabled"`
	}{disabled}
	return a.sendToLeader(ctx, http.MethodPost, disableURL, body, nil)
}

// ToggleTopicPartitions will toggle the given partitions in the given topic.
func (a *AdminAPI) ToggleTopicPartitions(ctx context.Context, disabled bool, namespace, topic string, partition int) error {
	disableURL := fmt.Sprintf("%v/%v/%v/%v", partitionsBaseURL, namespace, topic, partition)
	body := struct {
		Disabled bool `json:"disabled"`
	}{disabled}
	return a.sendToLeader(ctx, http.MethodPost, disableURL, body, nil)
}

// MajorityLostPartitions returns a list of partitions that have lost majority
// when given an input set of dead nodes.
func (a *AdminAPI) MajorityLostPartitions(ctx context.Context, deadNodes []int) ([]MajorityLostPartitions, error) {
	var rr []MajorityLostPartitions
	// One liner to convert []int -> int,int,int.
	csv := strings.Trim(strings.Join(strings.Split(fmt.Sprint(deadNodes), " "), ","), "[]")
	path := fmt.Sprintf("/v1/partitions/majority_lost?dead_nodes=%v", csv)
	return rr, a.sendAny(ctx, http.MethodGet, path, nil, &rr)
}

// ForceRecoverFromNode force recovers partitions from input list of nodes.
func (a *AdminAPI) ForceRecoverFromNode(ctx context.Context, plan []MajorityLostPartitions, deadNodes []int) error {
	body := map[string]interface{}{
		"dead_nodes":                  deadNodes,
		"partitions_to_force_recover": plan,
	}
	return a.sendAny(ctx, http.MethodPost, "/v1/partitions/force_recover_from_nodes", body, nil)
}

func (a *AdminAPI) TransferLeadership(ctx context.Context, fs afero.Fs, p *config.RpkProfile, source int, ns, topic string, partition int, target string) error {
	brokerURL, err := a.brokerIDToURL(ctx, source)
	if err != nil {
		return err
	}
	cl, err := NewHostClient(fs, p, brokerURL)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/v1/partitions/%s/%s/%d/transfer_leadership?target=%s", ns, topic, partition, target)
	return cl.sendOne(ctx, http.MethodPost, path, nil, nil, false)
}

// TriggerBalancer Trigger on-demand balancer.
func (a *AdminAPI) TriggerBalancer(ctx context.Context) error {
	return a.sendToLeader(ctx, http.MethodPost, "/v1/partitions/rebalance", nil, nil)
}
