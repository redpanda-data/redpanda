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
	"github.com/twmb/franz-go/pkg/kgo"
)

// parallel_driver_produce: produce a bounded random batch to a random test
// topic on the SOURCE cluster. Best-effort under fault injection — transient
// failures are expected and not bugs.
func produce() error {
	t := testTopics[randN(len(testTopics))]
	// A per-invocation nonce keeps this producer's keys distinct from every
	// other concurrent producer's, so (nonce, seq) uniquely identifies a
	// record. It also rides in the ClientID for request-log correlation.
	nonce := rng.Uint64()
	cl, err := srcCluster.newClient(
		kgo.ClientID(fmt.Sprintf("sl/produce/%016x", nonce)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		// Assign partitions ourselves so each record carries the topic and
		// partition it was written to; readers on both clusters verify it
		// was served from there.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// Flag franz-go's data-loss detection: it otherwise resets the
		// producer id and silently carries on, hiding an anomaly.
		kgo.ProducerOnDataLossDetected(reportDataLoss),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	count := 1 + randN(50)
	recs := make([]*kgo.Record, count)
	for i := range recs {
		recs[i] = makeRecord(nonce, i, t.name, int32(randN(topicPartitions)))
	}

	fmt.Printf("producing %d records to %s (nonce=%016x)\n", count, t.name, nonce)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		fmt.Printf("produce to %s did not complete (expected under faults) (nonce=%016x): %v\n",
			t.name, nonce, err)
		return nil
	}
	// Prove the workload actually writes data in some timeline; without this
	// a run where produce never succeeds would pass every safety check
	// vacuously, since the checkers only assert on non-empty reads.
	assert.Reachable("workload produced a batch to a shadow-linked topic",
		map[string]any{"topic": t.name, "count": count, "nonce": fmt.Sprintf("%016x", nonce)})
	fmt.Printf("produced %d records to %s (nonce=%016x)\n", count, t.name, nonce)
	return nil
}

// reportDataLoss is the franz-go ProducerOnDataLossDetected hook: an
// out-of-order sequence number or unknown producer id that franz-go cannot
// attribute to benign prefix truncation. A presumption, not proof, but on an
// acks=all producer it is an anomaly worth surfacing.
func reportDataLoss(topic string, part int32) {
	details := map[string]any{"topic": topic, "partition": part}
	assert.Unreachable("idempotent producer detected data loss (out-of-order sequence or unknown producer id)", details)
	fmt.Printf("data loss detected on %s/%d (out-of-order sequence or unknown producer id)\n", topic, part)
}
