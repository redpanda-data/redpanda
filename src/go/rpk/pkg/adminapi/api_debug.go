// Copyright 2023 Redpanda Data, Inc.
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

const (
	debugEndpoint          = "/v1/debug/self_test"
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

func (a *AdminAPI) StartSelfTest(ctx context.Context, nodeIds []int, params []any) (string, error) {
	var testID string
	body := SelfTestRequest{
		Tests: params,
		Nodes: nodeIds,
	}
	err := a.sendToLeader(ctx,
		http.MethodPost,
		fmt.Sprintf("%s/start", debugEndpoint),
		body,
		&testID)
	return testID, err
}

func (a *AdminAPI) StopSelfTest(ctx context.Context) error {
	return a.sendToLeader(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/stop", debugEndpoint),
		nil,
		nil,
	)
}

func (a *AdminAPI) SelfTestStatus(ctx context.Context) ([]SelfTestNodeReport, error) {
	var response []SelfTestNodeReport
	err := a.sendAny(ctx, http.MethodGet, fmt.Sprintf("%s/status", debugEndpoint), nil, &response)
	return response, err
}
