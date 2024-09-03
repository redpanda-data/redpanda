// Copyright 2023 Redpanda Data, Inc.
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
	"strconv"
	"time"
)

const (
	debugEndpoint          = "/v1/debug"
	selfTestEndpoint       = debugEndpoint + "/self_test"
	cpuProfilerEndpoint    = debugEndpoint + "/cpu_profile"
	DiskcheckTagIdentifier = "disk"
	NetcheckTagIdentifier  = "network"
)

// A SelfTestNodeResult describes the results of a particular self-test run.
// Currently there are only two types of tests, disk & network. The struct
// contains latency and throughput metrics as well as any errors or warnings
// that may have arisen during the tests execution.
type SelfTestNodeResult struct {
	P50            *uint   `json:"p50,omitempty"`
	P90            *uint   `json:"p90,omitempty"`
	P99            *uint   `json:"p99,omitempty"`
	P999           *uint   `json:"p999,omitempty"`
	MaxLatency     *uint   `json:"max_latency,omitempty"`
	RequestsPerSec *uint   `json:"rps,omitempty"`
	BytesPerSec    *uint   `json:"bps,omitempty"`
	Timeouts       uint    `json:"timeouts"`
	TestID         string  `json:"test_id"`
	TestName       string  `json:"name"`
	TestInfo       string  `json:"info"`
	TestType       string  `json:"test_type"`
	Duration       uint    `json:"duration"`
	Warning        *string `json:"warning,omitempty"`
	Error          *string `json:"error,omitempty"`
}

// SelfTestNodeReport describes the result returned from one member of the cluster.
// A query for results will return an array of these structs, one from each member.
type SelfTestNodeReport struct {
	NodeID int `json:"node_id"`
	// One of { "idle", "running", "unreachable" }
	//
	// If a status of idle is returned, the following `Results` variable will not
	// be nil. In all other cases it will be nil.
	Status string `json:"status"`
	// If value of `Status` is "idle", then this field will contain one result for
	// each peer involved in the test. It represents the results of the last
	// successful test run.
	Results []SelfTestNodeResult `json:"results,omitempty"`
}

// DiskcheckParameters describes what parameters redpanda will use when starting the diskcheck benchmark.
type DiskcheckParameters struct {
	/// Descriptive name given to test run
	Name string `json:"name"`
	// Open the file with O_DSYNC flag option
	DSync bool `json:"dsync"`
	// Set to true to disable the write portion of the benchmark
	SkipWrite bool `json:"skip_write"`
	// Set to true to disable the read portion of the benchmark
	SkipRead bool `json:"skip_read"`
	// Total size of all benchmark files to exist on disk
	DataSize uint `json:"data_size"`
	// Size of individual read and/or write requests
	RequestSize uint `json:"request_size"`
	// Total duration of the benchmark
	DurationMs uint `json:"duration_ms"`
	// Amount of fibers to run per shard
	Parallelism uint `json:"parallelism"`
	// Filled in automatically by the \ref StartSelfTest method
	Type string `json:"type"`
}

// NetcheckParameters describes what parameters redpanda will use when starting the netcheck benchmark.
type NetcheckParameters struct {
	/// Descriptive name given to test run
	Name string `json:"name"`
	// Size of individual request
	RequestSize uint `json:"request_size"`
	// Total duration of an individual benchmark
	DurationMs uint `json:"duration_ms"`
	// Number of fibers per shard used to make network requests
	Parallelism uint `json:"parallelism"`
	// Filled in automatically by the \ref StartSelfTest method
	Type string `json:"type"`
}

// SelfTestRequest represents the body of a self-test start POST request.
type SelfTestRequest struct {
	Tests []any `json:"tests,omitempty"`
	Nodes []int `json:"nodes,omitempty"`
}

// PartitionLeaderTable is the information about leaders, the information comes
// from the Redpanda's partition_leaders_table.
type PartitionLeaderTable struct {
	Ns                   string `json:"ns"`
	Topic                string `json:"topic"`
	PartitionID          int    `json:"partition_id"`
	Leader               int    `json:"leader"`
	PreviousLeader       int    `json:"previous_leader"`
	LastStableLeaderTerm int    `json:"last_stable_leader_term"`
	UpdateTerm           int    `json:"update_term"`
	PartitionRevision    int    `json:"partition_revision"`
}

// ControllerStatus is the status of a controller, as seen by a node.
type ControllerStatus struct {
	StartOffset       int `json:"start_offset"`
	LastAppliedOffset int `json:"last_applied_offset"`
	CommittedIndex    int `json:"committed_index"`
	DirtyOffset       int `json:"dirty_offset"`
}

// ReplicaState is the partition state of a replica. There are many keys
// returned, so the raw response is just unmarshalled into an interface.
type ReplicaState map[string]any

// DebugPartition is the low level debug information of a partition.
type DebugPartition struct {
	Ntp      string         `json:"ntp"`
	Replicas []ReplicaState `json:"replicas"`
}

func (a *AdminAPI) StartSelfTest(ctx context.Context, nodeIDs []int, params []any) (string, error) {
	var testID string
	body := SelfTestRequest{
		Tests: params,
		Nodes: nodeIDs,
	}
	err := a.sendToLeader(ctx,
		http.MethodPost,
		fmt.Sprintf("%s/start", selfTestEndpoint),
		body,
		&testID)
	return testID, err
}

func (a *AdminAPI) StopSelfTest(ctx context.Context) error {
	return a.sendToLeader(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/stop", selfTestEndpoint),
		nil,
		nil,
	)
}

func (a *AdminAPI) SelfTestStatus(ctx context.Context) ([]SelfTestNodeReport, error) {
	var response []SelfTestNodeReport
	err := a.sendAny(ctx, http.MethodGet, fmt.Sprintf("%s/status", selfTestEndpoint), nil, &response)
	return response, err
}

// PartitionLeaderTable returns the partitions leader table for the requested
// node.
func (a *AdminAPI) PartitionLeaderTable(ctx context.Context) ([]PartitionLeaderTable, error) {
	var response []PartitionLeaderTable
	return response, a.sendAny(ctx, http.MethodGet, "/v1/debug/partition_leaders_table", nil, &response)
}

func (a *AdminAPI) IsNodeIsolated(ctx context.Context) (bool, error) {
	var isIsolated bool
	return isIsolated, a.sendAny(ctx, http.MethodGet, "/v1/debug/is_node_isolated", nil, &isIsolated)
}

// ControllerStatus returns the controller status, as seen by the requested
// node.
func (a *AdminAPI) ControllerStatus(ctx context.Context) (ControllerStatus, error) {
	var response ControllerStatus
	return response, a.sendAny(ctx, http.MethodGet, "/v1/debug/controller_status", nil, &response)
}

// DebugPartition returns low level debug information (on any node) of all
// replicas of a given partition.
func (a *AdminAPI) DebugPartition(ctx context.Context, namespace, topic string, partitionID int) (DebugPartition, error) {
	var response DebugPartition
	return response, a.sendAny(ctx, http.MethodGet, fmt.Sprintf("/v1/debug/partition/%v/%v/%v", namespace, topic, partitionID), nil, &response)
}

// RawCPUProfile returns the raw response of the CPU profiler.
func (a *AdminAPI) RawCPUProfile(ctx context.Context, wait time.Duration) ([]byte, error) {
	var response []byte
	path := fmt.Sprintf("%v?wait_ms=%v", cpuProfilerEndpoint, strconv.Itoa(int(wait.Milliseconds())))
	return response, a.sendAny(ctx, http.MethodGet, path, nil, &response)
}
