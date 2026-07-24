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
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

// drainDeadline is how long the eventually check waits for the shadow link
// to catch up after faults stop, from SL_DRAIN_DEADLINE (Go duration).
func drainDeadline() time.Duration {
	if v := os.Getenv("SL_DRAIN_DEADLINE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		fmt.Printf("ignoring invalid SL_DRAIN_DEADLINE %q\n", v)
	}
	return 5 * time.Minute
}

// lagSnapshot compares source and target end offsets for every test
// topic/partition, each side answered by its own cluster's linearizable
// list_offsets. Returns whether every partition is drained (equal ends) and
// a per-partition lag map for assertion details.
func lagSnapshot() (bool, map[string]any) {
	drained := true
	lags := map[string]any{}
	pol := readPolicy{attempts: 1}
	for _, t := range testTopics {
		for part := range t.partitions {
			key := fmt.Sprintf("%s/%d", t.name, part)
			_, shi, err1 := partitionBounds(srcCluster, t.name, part, pol)
			_, dhi, err2 := partitionBounds(dstCluster, t.name, part, pol)
			if err1 != nil || err2 != nil {
				drained = false
				lags[key] = fmt.Sprintf("unreadable (src err=%v, dst err=%v)", err1, err2)
				continue
			}
			lag := shi - dhi
			lags[key] = lag
			if lag != 0 {
				drained = false
			}
		}
	}
	return drained, lags
}

// diffRecords compares two complete reads of the same partition range,
// source vs target. Shadow replication copies the log verbatim — same
// offsets, same key and value bytes (the values embed CRCs, so byte
// equality is checksum equality). Returns the first divergence.
func diffRecords(src, dst []*kgo.Record) (bool, string, int64) {
	if len(src) != len(dst) {
		return false, fmt.Sprintf("record count differs: source %d, target %d", len(src), len(dst)), -1
	}
	for i := range src {
		s, d := src[i], dst[i]
		if s.Offset != d.Offset {
			return false, fmt.Sprintf("offset mismatch at index %d: source %d, target %d", i, s.Offset, d.Offset), d.Offset
		}
		if !bytes.Equal(s.Key, d.Key) {
			return false, "key bytes differ", s.Offset
		}
		if !bytes.Equal(s.Value, d.Value) {
			return false, "value bytes differ (checksum mismatch)", s.Offset
		}
	}
	return true, "", -1
}

// comparePartition reads t/part fully from both clusters and asserts the
// target is byte-identical to the source. Bounds are re-fetched after the
// reads; if a high watermark moved (a straggler produce request from the
// fault window landing late), the attempt is retried rather than asserted,
// up to maxAttempts — on the last attempt everything is asserted as-is.
// Returns whether the partition verified identical and how many records it
// holds.
func comparePartition(t string, part int32) (bool, int) {
	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		final := attempt == maxAttempts
		pol := eventuallyRead()

		slo, shi, err1 := partitionBounds(srcCluster, t, part, pol)
		dlo, dhi, err2 := partitionBounds(dstCluster, t, part, pol)
		details := map[string]any{
			"topic": t, "partition": part, "attempt": attempt,
			"src_lo": slo, "src_hi": shi, "dst_lo": dlo, "dst_hi": dhi,
			"src_err": fmt.Sprint(err1), "dst_err": fmt.Sprint(err2),
		}
		if err1 != nil || err2 != nil {
			if !final {
				time.Sleep(5 * time.Second)
				continue
			}
			assert.Unreachable("eventually: partition bounds unreadable after faults stopped", details)
			return false, 0
		}

		boundsEqual := slo == dlo && shi == dhi
		if shi <= slo && boundsEqual {
			// Both empty: identical by definition.
			assert.Always(true, "eventually: shadow partition bounds match the source", details)
			return true, 0
		}

		var srcRecs, dstRecs []*kgo.Record
		srcComplete, dstComplete, stable := false, false, false
		if boundsEqual {
			srcRecs = readRange(srcCluster, t, part, slo, shi, pol)
			dstRecs = readRange(dstCluster, t, part, dlo, dhi, pol)
			srcComplete = int64(len(srcRecs)) == shi-slo && len(srcRecs) > 0 &&
				srcRecs[0].Offset == slo && srcRecs[len(srcRecs)-1].Offset == shi-1
			dstComplete = int64(len(dstRecs)) == dhi-dlo && len(dstRecs) > 0 &&
				dstRecs[0].Offset == dlo && dstRecs[len(dstRecs)-1].Offset == dhi-1

			_, shi2, e1 := partitionBounds(srcCluster, t, part, pol)
			_, dhi2, e2 := partitionBounds(dstCluster, t, part, pol)
			stable = e1 == nil && e2 == nil && shi2 == shi && dhi2 == dhi
		}
		if !final && (!boundsEqual || !stable || !srcComplete || !dstComplete) {
			fmt.Printf("eventually %s/%d attempt %d: bounds_equal=%v stable=%v src_complete=%v dst_complete=%v; retrying\n",
				t, part, attempt, boundsEqual, stable, srcComplete, dstComplete)
			time.Sleep(5 * time.Second)
			continue
		}

		details["stable"] = stable
		details["src_count"] = len(srcRecs)
		details["dst_count"] = len(dstRecs)
		assert.Always(boundsEqual, "eventually: shadow partition bounds match the source", details)
		if !boundsEqual {
			return false, 0
		}
		assert.Always(srcComplete && dstComplete && stable,
			"eventually: both clusters fully readable to a stable high watermark", details)

		// Shape validation on both sides (in-order, contiguous, intact,
		// ours, per-producer order).
		validateRange(srcCluster, t, part, slo, shi, slo, shi, srcRecs)
		validateRange(dstCluster, t, part, dlo, dhi, dlo, dhi, dstRecs)

		equal, reason, badOff := diffRecords(srcRecs, dstRecs)
		details["diff_reason"] = reason
		details["diff_offset"] = badOff
		assert.Always(equal, "eventually: shadow topic records are byte-identical to the source", details)

		fmt.Printf("eventually %s/%d [%d,%d) -> %d records (identical=%v reason=%q)\n",
			t, part, slo, shi, len(dstRecs), equal, reason)
		return equal && srcComplete && dstComplete && stable, len(dstRecs)
	}
}

// eventually_check_replicated: after Antithesis stops fault injection, wait
// for both clusters to recover, wait for the shadow link to drain (no
// producers run in this phase, so source high watermarks are quiescent), and
// then verify every partition of every test topic is byte-identical across
// the two clusters.
func checkReplicated() error {
	for _, c := range []*cluster{srcCluster, dstCluster} {
		if err := waitClusterReady(c, 5*time.Minute); err != nil {
			assert.Unreachable("eventually: cluster did not recover after faults stopped",
				map[string]any{"cluster": c.role, "err": err.Error()})
			return err
		}
	}

	// Phase 1: lag -> 0, computed from the two clusters' own answers rather
	// than the link's cached view of the source.
	deadline := time.Now().Add(drainDeadline())
	drained, lags := lagSnapshot()
	for !drained && time.Now().Before(deadline) {
		fmt.Printf("waiting for shadow link to drain: %v\n", lags)
		time.Sleep(5 * time.Second)
		drained, lags = lagSnapshot()
	}
	assert.Always(drained, "eventually: shadow link lag drains to zero after faults stop", lags)

	// The admin API can hiccup right after recovery; bound time, not
	// attempts (the eventually-phase policy), and assert the final answer.
	var st linkStatus
	var stErr error
	linkDeadline := time.Now().Add(2 * time.Minute)
	for {
		st, stErr = getShadowLink()
		if (stErr == nil && st.State == "SHADOW_LINK_STATE_ACTIVE") || time.Now().After(linkDeadline) {
			break
		}
		fmt.Printf("waiting for shadow link status: state=%q err=%v\n", st.State, stErr)
		time.Sleep(5 * time.Second)
	}
	linkActive := stErr == nil && st.State == "SHADOW_LINK_STATE_ACTIVE"
	assert.Always(linkActive, "eventually: shadow link is ACTIVE after faults stop",
		map[string]any{"state": st.State, "err": fmt.Sprint(stErr)})

	// Phase 2: per-partition byte comparison, with per-topic coverage
	// properties (three literal names so tsv2 coverage cannot hide behind
	// local passing, and vice versa).
	for _, t := range testTopics {
		identical := true
		records := 0
		for part := range t.partitions {
			ok, n := comparePartition(t.name, part)
			identical = identical && ok
			records += n
		}
		details := map[string]any{"topic": t.name, "records": records, "identical": identical}
		fmt.Printf("eventually %s: identical=%v records=%d\n", t.name, identical, records)
		switch t.name {
		case "tsv2":
			assert.Always(identical, "eventually: tsv2 shadow topic is identical to its source", details)
			if records > 0 {
				assert.Reachable("eventually: validated a non-empty tsv2 shadow topic", details)
			}
		case "cloud":
			assert.Always(identical, "eventually: cloud shadow topic is identical to its source", details)
			if records > 0 {
				assert.Reachable("eventually: validated a non-empty cloud shadow topic", details)
			}
		case "local":
			assert.Always(identical, "eventually: local shadow topic is identical to its source", details)
			if records > 0 {
				assert.Reachable("eventually: validated a non-empty local shadow topic", details)
			}
		default:
			assert.Unreachable("eventually: unknown test topic in coverage switch",
				map[string]any{"topic": t.name})
		}
	}
	return nil
}
