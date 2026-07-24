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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func rec(off int64, key, val string) *kgo.Record {
	return &kgo.Record{Offset: off, Key: []byte(key), Value: []byte(val)}
}

func TestDiffRecords(t *testing.T) {
	src := []*kgo.Record{rec(0, "k0", "v0"), rec(1, "k1", "v1")}

	if eq, reason, _ := diffRecords(src, []*kgo.Record{rec(0, "k0", "v0"), rec(1, "k1", "v1")}); !eq {
		t.Fatalf("identical logs reported different: %s", reason)
	}
	if eq, _, _ := diffRecords(src, src[:1]); eq {
		t.Error("count mismatch not caught")
	}
	if eq, _, off := diffRecords(src, []*kgo.Record{rec(0, "k0", "v0"), rec(2, "k1", "v1")}); eq || off != 2 {
		t.Errorf("offset mismatch not caught (off=%d)", off)
	}
	if eq, _, off := diffRecords(src, []*kgo.Record{rec(0, "k0", "v0"), rec(1, "kX", "v1")}); eq || off != 1 {
		t.Errorf("key mismatch not caught (off=%d)", off)
	}
	if eq, _, off := diffRecords(src, []*kgo.Record{rec(0, "k0", "v0"), rec(1, "k1", "vX")}); eq || off != 1 {
		t.Errorf("value mismatch not caught (off=%d)", off)
	}
	if eq, _, _ := diffRecords(nil, nil); !eq {
		t.Error("two empty logs must be equal")
	}
}

func TestDrainDeadline(t *testing.T) {
	os.Unsetenv("SL_DRAIN_DEADLINE")
	if d := drainDeadline(); d != 5*time.Minute {
		t.Errorf("default = %v, want 5m", d)
	}
	t.Setenv("SL_DRAIN_DEADLINE", "90s")
	if d := drainDeadline(); d != 90*time.Second {
		t.Errorf("env override = %v, want 90s", d)
	}
	t.Setenv("SL_DRAIN_DEADLINE", "bogus")
	if d := drainDeadline(); d != 5*time.Minute {
		t.Errorf("bad env = %v, want 5m fallback", d)
	}
}
