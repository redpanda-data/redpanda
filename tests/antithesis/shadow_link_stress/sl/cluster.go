// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// cluster identifies one side of the shadow link and how to reach it. All
// addresses come from the environment the compose template sets, so the same
// binary drives both clusters.
type cluster struct {
	role        string // "source" or "target"; carried in logs and assertion details
	brokersEnv  string // comma-separated Kafka addresses
	adminEnv    string // comma-separated admin API host:port
	expectedEnv string // broker count the workload waits for
}

var (
	srcCluster = &cluster{role: "source", brokersEnv: "SRC_KAFKA_BROKERS",
		adminEnv: "SRC_ADMIN_HOSTS", expectedEnv: "SRC_EXPECTED_BROKERS"}
	dstCluster = &cluster{role: "target", brokersEnv: "DST_KAFKA_BROKERS",
		adminEnv: "DST_ADMIN_HOSTS", expectedEnv: "DST_EXPECTED_BROKERS"}
)

func (c *cluster) brokers() []string {
	return strings.Split(os.Getenv(c.brokersEnv), ",")
}

func (c *cluster) adminHosts() []string {
	return strings.Split(os.Getenv(c.adminEnv), ",")
}

func (c *cluster) expected() int {
	if n, err := strconv.Atoi(os.Getenv(c.expectedEnv)); err == nil && n > 0 {
		return n
	}
	return 1
}

func (c *cluster) newClient(opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(c.brokers()...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	}
	return kgo.NewClient(append(base, opts...)...)
}
