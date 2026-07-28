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

func TestFlipTargetMode(t *testing.T) {
	for mode, want := range map[string]struct {
		target string
		ok     bool
	}{
		storageModeCloud:  {storageModeTiered, true},
		storageModeTiered: {storageModeCloud, true},
		"":                {"", false},
		"tiered_v2":       {"", false},
	} {
		target, ok := flipTargetMode(mode)
		if target != want.target || ok != want.ok {
			t.Errorf("flipTargetMode(%q) = (%q, %v), want (%q, %v)",
				mode, target, ok, want.target, want.ok)
		}
	}
}

func TestFlippableTopics(t *testing.T) {
	names := make([]string, 0, 2)
	for _, tt := range flippableTopics() {
		names = append(names, tt.name)
	}
	// local is the no-remote-storage control topic and must never be flipped.
	if want := []string{"tsv2", "cloud"}; !slices.Equal(names, want) {
		t.Errorf("flippableTopics() = %v, want %v", names, want)
	}
}
