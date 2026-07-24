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
	"slices"
	"testing"
)

func TestIsTestCommand(t *testing.T) {
	for name, want := range map[string]bool{
		"first_create_topics_and_link": true,
		"parallel_driver_produce":      true,
		"anytime_check_target":         true,
		"eventually_check_replicated":  true,
		"setup":                        false,
		"list-commands":                false,
	} {
		if got := isTestCommand(name); got != want {
			t.Errorf("isTestCommand(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestQuiescedPhase(t *testing.T) {
	cmdName = "eventually_check_replicated"
	if !quiescedPhase() {
		t.Error("eventually_ command must report quiesced phase")
	}
	cmdName = "anytime_check_target"
	if quiescedPhase() {
		t.Error("anytime_ command must not report quiesced phase")
	}
}

func TestTestTopics(t *testing.T) {
	names := make([]string, 0, len(testTopics))
	for _, tt := range testTopics {
		names = append(names, tt.name)
		if tt.partitions != topicPartitions || tt.replicas != topicReplicas {
			t.Errorf("topic %s: partitions/replicas = %d/%d", tt.name, tt.partitions, tt.replicas)
		}
	}
	want := []string{"tsv2", "cloud", "local"}
	if !slices.Equal(names, want) {
		t.Errorf("testTopics = %v, want %v", names, want)
	}
	if m := testTopics[0].config["redpanda.storage.mode"]; m == nil || *m != "tiered" {
		t.Error("tsv2 must set redpanda.storage.mode=tiered")
	}
	if m := testTopics[1].config["redpanda.storage.mode"]; m == nil || *m != "cloud" {
		t.Error("cloud must set redpanda.storage.mode=cloud")
	}
	if len(testTopics[2].config) != 0 {
		t.Error("local must set no topic config")
	}
}
