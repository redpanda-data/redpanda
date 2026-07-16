// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

// testTopic describes one topic the workload creates and exercises.
type testTopic struct {
	name       string
	partitions int32
	replicas   int16
	compacted  bool
}

// testTopics is the single definition of the workload's topics; the
// create/flip/move drivers all derive from it.
var testTopics = []testTopic{
	{name: fooTopic, partitions: fooPartitions, replicas: fooReplicas},
	{name: ctcTopic, partitions: ctcPartitions, replicas: ctcReplicas, compacted: true},
}
