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

// readPolicy bounds a retried read; a zero value leaves the corresponding
// bound off. Anytime commands bound attempts and leave time open: on failure
// they stop, and the test composer reschedules the command, so persistence
// comes from scheduling. The eventually check bounds time and leaves attempts
// open: it runs once, after faults stop, and has to wait out post-fault
// recovery.
type readPolicy struct {
	attempts int       // max tries; 0 = unlimited
	deadline time.Time // no retry after this instant; zero = no deadline
}

func (p readPolicy) exhausted(attempt int) bool {
	if p.attempts > 0 && attempt >= p.attempts {
		return true
	}
	return !p.deadline.IsZero() && time.Now().After(p.deadline)
}

func anytimeRead() readPolicy { return readPolicy{attempts: 3} }

// partitionBounds returns a partition's current start and high-watermark
// offsets, retrying failed lookups per pol. Records occupy [lo, hi); the
// partition is empty when hi <= lo. A successful answer is never retried,
// empty or not: list_offsets replies only after a linearizable barrier
// (kafka/server/handlers/list_offsets.cc), so it is authoritative.
func partitionBounds(t string, part int32, pol readPolicy) (lo, hi int64, err error) {
	for attempt := 1; ; attempt++ {
		lo, hi, err = partitionBoundsOnce(t, part)
		if err == nil || pol.exhausted(attempt) {
			return lo, hi, err
		}
		fmt.Printf("bounds of %s/%d attempt %d: %v; retrying\n", t, part, attempt, err)
		time.Sleep(2 * time.Second)
	}
}

func partitionBoundsOnce(t string, part int32) (lo, hi int64, err error) {
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

	// A missing entry (e.g. a topic-level metadata error while the cluster
	// recovers) must be an error: the zero value would read as an empty
	// partition at offset 0.
	s, ok := starts.Lookup(t, part)
	if !ok {
		return 0, 0, fmt.Errorf("%s/%d missing from start-offsets response", t, part)
	}
	if s.Err != nil {
		return 0, 0, s.Err
	}
	e, ok := ends.Lookup(t, part)
	if !ok {
		return 0, 0, fmt.Errorf("%s/%d missing from end-offsets response", t, part)
	}
	if e.Err != nil {
		return 0, 0, e.Err
	}
	return s.Offset, e.Offset, nil
}

// readRange consumes offsets [o1, o2) from a single partition, retrying per
// pol until a read reaches o2-1. Returns the records of the last attempt in
// the order the broker served them: the whole range on success, a prefix if
// the policy ran out first.
func readRange(t string, part int32, o1, o2 int64, pol readPolicy) []*kgo.Record {
	for attempt := 1; ; attempt++ {
		recs := readRangeOnce(t, part, o1, o2)
		if len(recs) > 0 && recs[len(recs)-1].Offset == o2-1 {
			return recs
		}
		if pol.exhausted(attempt) {
			return recs
		}
		fmt.Printf("read of %s/%d [%d,%d) attempt %d stopped after %d records; retrying\n",
			t, part, o1, o2, attempt, len(recs))
		time.Sleep(2 * time.Second)
	}
}

// readRangeOnce is one bounded attempt: it stops at the first fetch error or
// at its timeout and returns whatever arrived, so a fault that stalls the
// range cannot hang the command.
func readRangeOnce(t string, part int32, o1, o2 int64) []*kgo.Record {
	cl, err := newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		t: {part: kgo.NewOffset().At(o1)},
	}))
	if err != nil {
		return nil
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
	return recs
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
	pol := anytimeRead()
	part := int32(randN(fooPartitions))
	lo, hi, err := partitionBounds(fooTopic, part, pol)
	if err != nil || hi <= lo {
		return nil // empty or unreadable under faults; nothing to check
	}
	o1 := lo + int64(randN(int(hi-lo)))
	o2 := o1 + 1 + int64(randN(int(hi-o1)))

	recs := readRange(fooTopic, part, o1, o2, pol)
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
	if !quiescedPhase() {
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

	pol := anytimeRead()
	lo, hi, err := partitionBounds(fooTopic, int32(part), pol)
	if err != nil {
		fmt.Printf("check_offsets_foo: could not read bounds for foo/%d: %v\n", part, err)
		lo, hi = -1, -1
	}
	recs := readRange(fooTopic, int32(part), o1, o2, pol)
	fmt.Printf("check_offsets_foo: foo/%d [%d,%d) bounds=[%d,%d) -> %d records\n",
		part, o1, o2, lo, hi, len(recs))
	validateFooRange(int32(part), lo, hi, o1, o2, recs)
	return nil
}
