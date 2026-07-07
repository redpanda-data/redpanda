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
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// partitionBounds returns a partition's current start and high-watermark
// offsets. Records occupy [lo, hi); the partition is empty when hi <= lo.
func partitionBounds(part int32) (lo, hi int64, err error) {
	cl, err := newClient()
	if err != nil {
		return 0, 0, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, err
	}

	s, ok := starts.Lookup(topic, part)
	if !ok || s.Err != nil {
		return 0, 0, s.Err
	}
	e, ok := ends.Lookup(topic, part)
	if !ok || e.Err != nil {
		return 0, 0, e.Err
	}
	return s.Offset, e.Offset, nil
}

// randomPartitionRange picks a random partition and returns its current
// start and high-watermark offsets.
func randomPartitionRange() (part int32, lo, hi int64, err error) {
	part = int32(randN(fooPartitions))
	lo, hi, err = partitionBounds(part)
	return part, lo, hi, err
}

// readRange consumes offsets [o1, o2) from a single partition and returns
// them in the order the broker served them. Bounded by a timeout so a fault
// that stalls the range cannot hang the command.
func readRange(part int32, o1, o2 int64) ([]int64, error) {
	cl, err := newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {part: kgo.NewOffset().At(o1)},
	}))
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var offs []int64
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
			offs = append(offs, r.Offset)
			if r.Offset == o2-1 {
				done = true
			}
		}
		if done {
			break
		}
	}
	return offs, nil
}

// anytime_check_range: read a random range and assert the offsets it returns
// are a well-formed slice of the log. The safety invariants hold on whatever
// prefix comes back, so they never false-positive when a fault truncates the
// read; completeness is only a liveness (Sometimes) property.
//
// Contiguity assumes non-transactional produce with cleanup.policy=delete
// (our setup); markers or compaction would create legal gaps.
func check() error {
	part, lo, hi, err := randomPartitionRange()
	if err != nil || hi <= lo {
		return nil // empty or unreadable under faults; nothing to check
	}
	o1 := lo + int64(randN(int(hi-lo)))
	o2 := o1 + 1 + int64(randN(int(hi-o1)))

	offs, err := readRange(part, o1, o2)
	if err != nil || len(offs) == 0 {
		return nil
	}

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

	details := map[string]any{
		"partition": part, "o1": o1, "o2": o2,
		"count": len(offs), "first": first, "last": last, "lo": lo, "hi": hi,
	}

	assert.Reachable("checker read a non-empty cloud topic range", details)
	assert.Always(inOrder, "cloud topic range read returns in-order offsets", details)
	assert.Always(contiguous, "cloud topic range read returns contiguous offsets", details)
	assert.Always(withinBounds, "cloud topic range read stays within requested bounds", details)
	assert.Sometimes(full, "cloud topic range read returns the full requested range", details)

	fmt.Printf("checked foo/%d offsets %d:%d -> %d records (in_order=%v contiguous=%v bounds=%v full=%v)\n",
		part, o1, o2, len(offs), inOrder, contiguous, withinBounds, full)
	return nil
}

// readPartitionToEnd re-reads a partition's bounds and consumes the whole
// [lo, hi) range, retrying until the read reaches the high watermark or the
// deadline passes. The retries only cover the post-fault recovery window (a
// finally command runs after faults stop but the cluster may still be settling
// and there is no concurrent produce, so hi is stable). Returns the last-seen
// bounds and whatever the final attempt read: the complete slice on success, or
// a prefix if it gave up.
func readPartitionToEnd(part int32, deadline time.Time) (lo, hi int64, offs []int64) {
	for {
		var err error
		lo, hi, err = partitionBounds(part)
		if err == nil {
			if hi <= lo {
				return lo, hi, nil // empty partition
			}
			offs, err = readRange(part, lo, hi)
			if err == nil && int64(len(offs)) == hi-lo {
				return lo, hi, offs
			}
		}
		if time.Now().After(deadline) {
			return lo, hi, offs
		}
		time.Sleep(2 * time.Second)
	}
}

// finally_check_complete: after Antithesis stops fault injection for the
// timeline, wait for the cluster to recover, then read every partition of foo
// from its start offset to its high watermark and assert the log is intact —
// offsets strictly increasing, contiguous (no gaps), and readable all the way
// to the high watermark.
//
// Because this runs on a quiesced, healed cluster with no concurrent produce or
// faults, completeness is a hard Always here — contrast anytime_check_range,
// where a fault can truncate the read so completeness is only Sometimes.
//
// Contiguity assumes non-transactional produce with cleanup.policy=delete (our
// setup); markers or compaction would create legal gaps.
func checkComplete() error {
	// Faults stop when a finally command starts, but containers need time to
	// come back. Refuse to validate until the cluster serves metadata again;
	// a cluster that never recovers is itself a failure.
	if err := waitClusterReady(expectedBrokers(), 5*time.Minute); err != nil {
		assert.Unreachable("finally: cluster did not recover after faults stopped",
			map[string]any{"err": err.Error()})
		return err
	}

	for part := range int32(fooPartitions) {
		lo, hi, offs := readPartitionToEnd(part, time.Now().Add(2*time.Minute))
		if hi <= lo {
			continue // empty partition; nothing to verify
		}

		first, last := int64(-1), int64(-1)
		if len(offs) > 0 {
			first, last = offs[0], offs[len(offs)-1]
		}
		inOrder, contiguous := true, true
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				inOrder = false
			}
			if offs[i] != offs[i-1]+1 {
				contiguous = false
			}
		}
		complete := first == lo && last == hi-1 && int64(len(offs)) == hi-lo

		details := map[string]any{
			"partition": part, "lo": lo, "hi": hi,
			"count": len(offs), "first": first, "last": last,
		}
		assert.Reachable("finally: validated a non-empty cloud topic partition", details)
		assert.Always(inOrder, "finally: cloud topic partition offsets are strictly increasing", details)
		assert.Always(contiguous, "finally: cloud topic partition offsets are contiguous (no gaps)", details)
		assert.Always(complete, "finally: cloud topic partition is fully readable to the high watermark", details)

		fmt.Printf("finally foo/%d [%d,%d) -> %d records (in_order=%v contiguous=%v complete=%v)\n",
			part, lo, hi, len(offs), inOrder, contiguous, complete)
	}
	return nil
}
