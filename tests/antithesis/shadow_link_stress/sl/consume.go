// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

// checkRandomRange picks a random topic, partition and sub-range on cluster
// c and validates whatever the read returns. Empty or unreadable under
// faults is fine — nothing to check; persistence comes from scheduling.
func checkRandomRange(c *cluster) error {
	pol := anytimeRead()
	t := testTopics[randN(len(testTopics))].name
	part := int32(randN(topicPartitions))
	lo, hi, err := partitionBounds(c, t, part, pol)
	if err != nil || hi <= lo {
		return nil
	}
	o1 := lo + int64(randN(int(hi-lo)))
	o2 := o1 + 1 + int64(randN(int(hi-o1)))

	recs := readRange(c, t, part, o1, o2, pol)
	validateRange(c, t, part, lo, hi, o1, o2, recs)
	return nil
}

// parallel_driver_consume_source: validate source reads so a target
// divergence is attributable (source already bad vs corrupted in
// replication).
func checkSourceRange() error { return checkRandomRange(srcCluster) }

// anytime_check_target: validate reads served by the shadow topics. Uses the
// target's own bounds, so replication lag never false-positives.
func checkTargetRange() error { return checkRandomRange(dstCluster) }
