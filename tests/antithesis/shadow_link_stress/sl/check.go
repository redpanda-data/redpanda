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

// readPolicy bounds a retried read; a zero value leaves the corresponding
// bound off. Anytime commands bound attempts and leave time open: on failure
// they stop and the test composer reschedules the command, so persistence
// comes from scheduling. The eventually check bounds time and leaves attempts
// open: it runs once, after faults stop, and has to wait out recovery.
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

func eventuallyRead() readPolicy {
	return readPolicy{deadline: time.Now().Add(2 * time.Minute)}
}

// partitionBounds returns a partition's current start and high-watermark
// offsets on cluster c, retrying failed lookups per pol. Records occupy
// [lo, hi); the partition is empty when hi <= lo. A successful answer is
// never retried: list_offsets replies only after a linearizable barrier, so
// it is authoritative.
func partitionBounds(c *cluster, t string, part int32, pol readPolicy) (lo, hi int64, err error) {
	for attempt := 1; ; attempt++ {
		lo, hi, err = partitionBoundsOnce(c, t, part)
		if err == nil || pol.exhausted(attempt) {
			return lo, hi, err
		}
		fmt.Printf("bounds of %s %s/%d attempt %d: %v; retrying\n", c.role, t, part, attempt, err)
		time.Sleep(2 * time.Second)
	}
}

func partitionBoundsOnce(c *cluster, t string, part int32) (lo, hi int64, err error) {
	cl, err := c.newClient()
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

// readRange consumes offsets [o1, o2) from one partition on cluster c,
// retrying per pol until a read reaches o2-1. Returns the records of the
// last attempt in the order the broker served them: the whole range on
// success, a prefix if the policy ran out first.
func readRange(c *cluster, t string, part int32, o1, o2 int64, pol readPolicy) []*kgo.Record {
	for attempt := 1; ; attempt++ {
		recs := readRangeOnce(c, t, part, o1, o2)
		if len(recs) > 0 && recs[len(recs)-1].Offset == o2-1 {
			return recs
		}
		if pol.exhausted(attempt) {
			return recs
		}
		fmt.Printf("read of %s %s/%d [%d,%d) attempt %d stopped after %d records; retrying\n",
			c.role, t, part, o1, o2, attempt, len(recs))
		time.Sleep(2 * time.Second)
	}
}

// readRangeOnce is one bounded attempt: it stops at the first fetch error or
// at its timeout and returns whatever arrived, so a fault that stalls the
// range cannot hang the command.
func readRangeOnce(c *cluster, t string, part int32, o1, o2 int64) []*kgo.Record {
	cl, err := c.newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		t: {part: kgo.NewOffset().At(o1)},
	}))
	if err != nil {
		fmt.Printf("read of %s %s/%d: client: %v\n", c.role, t, part, err)
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

// validateRange asserts that recs — the result of reading [o1, o2) from
// t/part on cluster c — form a well-formed slice of the log: offsets in
// order, contiguous, within the requested bounds, and records that are
// intact, ours, and in per-producer order. All topics are delete-policy and
// non-transactional, so contiguity holds on both clusters (shadow
// replication copies offsets verbatim). An empty read is a no-op: the
// invariants hold on whatever prefix a fault leaves behind, so they never
// false-positive on truncation; completeness is a liveness property.
func validateRange(c *cluster, t string, part int32, lo, hi, o1, o2 int64, recs []*kgo.Record) {
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
	v := verifyRecords(t, part, recs)

	details := map[string]any{
		"command": cmdName, "cluster": c.role, "topic": t, "partition": part,
		"o1": o1, "o2": o2, "count": len(offs), "first": first, "last": last,
		"lo": lo, "hi": hi, "bad_data": v.bad, "reordered": v.reordered,
		"bad_offset": v.firstOff, "bad_reason": v.firstReason,
	}

	assert.Always(inOrder, "shadow-link topic range read returns in-order offsets", details)
	assert.Always(contiguous, "shadow-link topic range read returns contiguous offsets", details)
	assert.Always(withinBounds, "shadow-link topic range read stays within requested bounds", details)
	assert.Always(v.bad == 0, "shadow-link topic records are intact and self-consistent", details)
	assert.Always(v.reordered == 0, "shadow-link topic per-producer produce order is preserved", details)
	if !quiescedPhase() {
		assert.Reachable("checker read a non-empty shadow-link topic range", details)
		assert.Sometimes(full, "shadow-link topic range read returns the full requested range", details)
	}

	fmt.Printf("checked %s %s/%d offsets %d:%d -> %d records (in_order=%v contiguous=%v bounds=%v full=%v bad_data=%d reordered=%d)\n",
		c.role, t, part, o1, o2, len(offs), inOrder, contiguous, withinBounds, full, v.bad, v.reordered)
}
