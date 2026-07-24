// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

// testTopic describes one topic the workload creates on the source and
// validates on both clusters. The three names double as the storage-mode
// labels used in the eventually assertions.
type testTopic struct {
	name       string
	partitions int32
	replicas   int16
	// Topic configs applied at creation on the source. The shadow topic
	// inherits the storage mode automatically: the cluster_link topic
	// reconciler carries redpanda.storage.mode into the mirror topic config
	// (it is not part of the user-settable synced-properties list).
	config map[string]*string
}

const (
	topicPartitions = 3
	topicReplicas   = 3
)

// testTopics is the single definition of the workload's topics; creation,
// produce, checks and the eventually comparison all derive from it.
var testTopics = []testTopic{
	{name: "tsv2", partitions: topicPartitions, replicas: topicReplicas,
		config: map[string]*string{"redpanda.storage.mode": new("tiered")}},
	{name: "cloud", partitions: topicPartitions, replicas: topicReplicas,
		config: map[string]*string{"redpanda.storage.mode": new("cloud")}},
	{name: "local", partitions: topicPartitions, replicas: topicReplicas,
		config: map[string]*string{}},
}
