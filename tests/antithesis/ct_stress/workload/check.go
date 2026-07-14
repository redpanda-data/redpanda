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
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// partitionBounds returns a partition's current start and high-watermark
// offsets. Records occupy [lo, hi); the partition is empty when hi <= lo.
func partitionBounds(t string, part int32) (lo, hi int64, err error) {
	cl, err := newClient()
	if err != nil {
		return 0, 0, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	starts, err := adm.ListStartOffsets(ctx, t)
	if err != nil {
		return 0, 0, err
	}
	ends, err := adm.ListEndOffsets(ctx, t)
	if err != nil {
		return 0, 0, err
	}

	s, ok := starts.Lookup(t, part)
	if !ok || s.Err != nil {
		return 0, 0, s.Err
	}
	e, ok := ends.Lookup(t, part)
	if !ok || e.Err != nil {
		return 0, 0, e.Err
	}
	return s.Offset, e.Offset, nil
}

// randomFooPartitionRange picks a random foo partition and returns its current
// start and high-watermark offsets.
func randomFooPartitionRange() (part int32, lo, hi int64, err error) {
	part = int32(randN(fooPartitions))
	lo, hi, err = partitionBounds(fooTopic, part)
	return part, lo, hi, err
}

// readRange consumes offsets [o1, o2) from a single partition and returns the
// records in the order the broker served them. Bounded by a timeout so a fault
// that stalls the range cannot hang the command.
func readRange(t string, part int32, o1, o2 int64) ([]*kgo.Record, error) {
	cl, err := newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		t: {part: kgo.NewOffset().At(o1)},
	}))
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var recs []*kgo.Record
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break // timed out; return whatever we have
		}
		if errs := fs.Errors(); len(errs) > 0 {
			break // transient under faults
		}
		done := false
		iter := fs.RecordIter()
		for !iter.Done() {
			r := iter.Next()
			if r.Offset >= o2 {
				done = true
				break
			}
			recs = append(recs, r)
			if r.Offset == o2-1 {
				done = true
			}
		}
		if done {
			break
		}
	}
	return recs, nil
}

// offsets extracts the offsets of recs in order.
func offsets(recs []*kgo.Record) []int64 {
	offs := make([]int64, len(recs))
	for i, r := range recs {
		offs[i] = r.Offset
	}
	return offs
}

// checkFoo backs both anytime_check_range_foo and parallel_driver_consume_foo:
// pick a random sub-range of a random foo partition and validate the records
// it returns.
func checkFoo() error {
	part, lo, hi, err := randomFooPartitionRange()
	if err != nil || hi <= lo {
		return nil // empty or unreadable under faults; nothing to check
	}
	o1 := lo + int64(randN(int(hi-lo)))
	o2 := o1 + 1 + int64(randN(int(hi-o1)))

	recs, err := readRange(fooTopic, part, o1, o2)
	if err != nil {
		return nil
	}
	validateFooRange(part, lo, hi, o1, o2, recs)
	return nil
}

// validateFooRange asserts that recs — the result of reading [o1, o2) from part —
// form a well-formed slice of the log: offsets in order, contiguous, within the
// requested bounds, and records that are intact, ours, and in per-producer
// order. lo/hi are the partition's bounds, carried only for context in the
// assertion details. An empty read is a no-op: the invariants hold on whatever
// prefix a fault leaves behind, so they never false-positive on truncation, and
// completeness is only a liveness (Sometimes) property.
//
// Contiguity assumes non-transactional produce with cleanup.policy=delete
// (our setup); markers or compaction would create legal gaps.
func validateFooRange(part int32, lo, hi, o1, o2 int64, recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	offs := offsets(recs)

	first, last := offs[0], offs[len(offs)-1]
	inOrder, contiguous := true, true
	for i := 1; i < len(offs); i++ {
		if offs[i] <= offs[i-1] {
			inOrder = false
		}
		if offs[i] != offs[i-1]+1 {
			contiguous = false
		}
	}
	withinBounds := first >= o1 && last < o2
	full := first == o1 && int64(len(offs)) == o2-o1
	v := verifyFooRecords(part, recs)

	details := map[string]any{
		"command": cmdName, "partition": part, "o1": o1, "o2": o2,
		"count": len(offs), "first": first, "last": last, "lo": lo, "hi": hi,
		"bad_data": v.bad, "reordered": v.reordered,
		"bad_offset": v.firstOff, "bad_reason": v.firstReason,
	}

	assert.Always(inOrder, "delete-policy cloud topic range read returns in-order offsets", details)
	assert.Always(contiguous, "delete-policy cloud topic range read returns contiguous offsets", details)
	assert.Always(withinBounds, "delete-policy cloud topic range read stays within requested bounds", details)
	assert.Always(v.bad == 0, "delete-policy cloud topic records are intact and self-consistent", details)
	assert.Always(v.reordered == 0, "delete-policy cloud topic per-producer produce order is preserved", details)
	if !finallyPhase() {
		assert.Reachable("checker read a non-empty delete-policy cloud topic range", details)
		assert.Sometimes(full, "delete-policy cloud topic range read returns the full requested range", details)
	}

	fmt.Printf("checked foo/%d offsets %d:%d -> %d records (in_order=%v contiguous=%v bounds=%v full=%v bad_data=%d reordered=%d)\n",
		part, o1, o2, len(offs), inOrder, contiguous, withinBounds, full, v.bad, v.reordered)
}

// checkFooOffsets is a manual command (no test-composer prefix, so it gets no
// symlink and Antithesis never schedules it): read and validate an explicit
// [o1, o2) range on a given partition. Unlike the random anytime_check_range_foo,
// the range is fixed by its arguments, so the read replays verbatim. Under the
// multiverse debugger this lets you roll back to different points in a timeline
// and repeat the exact same read to pin down when a range first goes bad.
//
// Usage: helper_workload check_offsets_foo <partition> <o1> <o2>
func checkFooOffsets() error {
	if len(cmdArgs) != 3 {
		return fmt.Errorf("usage: check_offsets_foo <partition> <o1> <o2>")
	}
	part, err := strconv.ParseInt(cmdArgs[0], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid partition %q: %w", cmdArgs[0], err)
	}
	o1, err := strconv.ParseInt(cmdArgs[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid o1 %q: %w", cmdArgs[1], err)
	}
	o2, err := strconv.ParseInt(cmdArgs[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid o2 %q: %w", cmdArgs[2], err)
	}
	if o2 <= o1 {
		return fmt.Errorf("empty range: o2 (%d) must be > o1 (%d)", o2, o1)
	}

	lo, hi, err := partitionBounds(fooTopic, int32(part))
	if err != nil {
		fmt.Printf("check_offsets_foo: could not read bounds for foo/%d: %v\n", part, err)
		lo, hi = -1, -1
	}
	recs, err := readRange(fooTopic, int32(part), o1, o2)
	if err != nil {
		return fmt.Errorf("read of foo/%d [%d,%d) failed: %w", part, o1, o2, err)
	}
	fmt.Printf("check_offsets_foo: foo/%d [%d,%d) bounds=[%d,%d) -> %d records\n",
		part, o1, o2, lo, hi, len(recs))
	validateFooRange(int32(part), lo, hi, o1, o2, recs)
	return nil
}

// readPartitionToEnd re-reads a partition's bounds and consumes the whole
// [lo, hi) range, retrying until the read reaches the high watermark (a
// record at offset hi-1; on a compacted topic that is fewer than hi-lo
// records) or the deadline passes. The retries only cover the post-fault
// recovery window (a finally command runs after faults stop but the cluster
// may still be settling and there is no concurrent produce, so hi is stable).
// Returns the last-seen bounds and whatever the final attempt read: the
// complete slice on success, or a prefix if it gave up.
func readPartitionToEnd(t string, part int32, deadline time.Time) (lo, hi int64, recs []*kgo.Record) {
	for {
		var err error
		lo, hi, err = partitionBounds(t, part)
		if err == nil {
			if hi <= lo {
				return lo, hi, nil // empty partition
			}
			recs, err = readRange(t, part, lo, hi)
			if err == nil && len(recs) > 0 && recs[len(recs)-1].Offset == hi-1 {
				return lo, hi, recs
			}
		}
		if time.Now().After(deadline) {
			return lo, hi, recs
		}
		time.Sleep(2 * time.Second)
	}
}

// finally_check_complete: after Antithesis stops fault injection for the
// timeline, wait for the cluster to recover, then read every partition of
// every test topic from its start offset to its high watermark, assert the
// read is complete, and run the same shape validation the anytime checkers
// use: validateFooRange for the delete-policy topic (in-order, contiguous,
// intact, per-producer order), validateCtcRange for the compacted one (same
// minus contiguity, since compaction leaves legal gaps).
//
// Because this runs on a quiesced, healed cluster with no concurrent produce
// or faults, completeness is a hard Always here (one property per cleanup
// policy, since the predicate differs) — contrast the anytime checkers, where
// a fault can truncate a read so completeness is only Sometimes. Complete
// means the read reaches the high watermark; on the
// delete-policy topic it also means every offset in [lo, hi) is present,
// while compaction legally removes records anywhere, including at lo (the
// record at hi-1 is the newest and thus the latest for its key, so it always
// survives).
func checkComplete() error {
	// Faults stop when a finally command starts, but containers need time to
	// come back. Refuse to validate until the cluster serves metadata again;
	// a cluster that never recovers is itself a failure.
	if err := waitClusterReady(expectedBrokers(), 5*time.Minute); err != nil {
		assert.Unreachable("finally: cluster did not recover after faults stopped",
			map[string]any{"err": err.Error()})
		return err
	}

	// No producers run in the finally phase, so the acked summaries are the
	// final word on what the brokers acknowledged.
	ackedSnap, err := loadCtcAckedSnapshot()
	if err != nil {
		return err
	}

	for _, t := range testTopics {
		for part := range t.partitions {
			fmt.Printf("finally: checking %s/%d\n", t.name, part)

			lo, hi, recs := readPartitionToEnd(t.name, part, time.Now().Add(2*time.Minute))
			if hi <= lo {
				// An empty partition is only innocent if nothing was ever
				// acked on it; acked keys with no log at all are loss.
				if t.compacted {
					validateCtcAcked(part, hi, nil, ackedSnap, true)
				}
				continue // empty partition; nothing else to verify
			}

			first, last := int64(-1), int64(-1)
			if len(recs) > 0 {
				first, last = recs[0].Offset, recs[len(recs)-1].Offset
			}
			complete := last == hi-1
			if !t.compacted {
				complete = complete && first == lo && int64(len(recs)) == hi-lo
			}

			details := map[string]any{
				"topic": t.name, "partition": part, "lo": lo, "hi": hi,
				"count": len(recs), "first": first, "last": last,
			}
			fmt.Printf("finally %s/%d [%d,%d) -> %d records (complete=%v)\n",
				t.name, part, lo, hi, len(recs), complete)

			// Reachability and completeness are per cleanup policy: distinct
			// names keep each topic's finally coverage a separate obligation
			// (foo validating must not mask ctc never getting validated), and
			// the completeness predicate differs between the policies anyway.
			if t.compacted {
				assert.Reachable("finally: validated a non-empty compacted cloud topic partition", details)
				assert.Always(complete, "finally: compacted cloud topic partition is readable to the high watermark", details)
				latest, rangeComplete := validateCtcRange(part, lo, hi, recs)
				// The cluster is quiesced, so hi cannot move mid-read; a
				// complete read alone makes the loss check sound here.
				validateCtcAcked(part, hi, latest, ackedSnap, rangeComplete)
			} else {
				assert.Reachable("finally: validated a non-empty delete-policy cloud topic partition", details)
				assert.Always(complete, "finally: delete-policy cloud topic partition is fully readable to the high watermark", details)
				validateFooRange(part, lo, hi, lo, hi, recs)
			}
		}
	}
	return nil
}
