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
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// rng draws from Antithesis-controlled entropy so the platform both controls
// and can steer every choice; outside Antithesis it falls back to crypto/rand.
// Each Intn consults the source live, which is the per-decision use the SDK
// wants (no seeding, no caching draws for later).
var rng = rand.New(random.Source())

// randN returns a uniformly random int in [0, n).
func randN(n int) int {
	if n <= 0 {
		return 0
	}
	return rng.Intn(n)
}

// first_create_topic: create the cloud topic the workload exercises. Runs
// once per timeline after setup_complete; must not signal lifecycle itself.
func createTopic() error {
	cl, err := newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx := context.Background()

	cloud := "cloud"
	cfg := map[string]*string{"redpanda.storage.mode": &cloud}

	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := adm.CreateTopics(ctx, fooPartitions, fooReplicas, cfg, topic)
		if err == nil {
			if terr := resp[topic].Err; terr == nil {
				fmt.Printf("created cloud topic foo (%d partitions, %d replicas)\n",
					fooPartitions, fooReplicas)
				return nil
			} else if errors.Is(terr, kerr.TopicAlreadyExists) {
				fmt.Println("cloud topic foo already exists")
				return nil
			} else {
				err = terr
			}
		}
		fmt.Printf("attempt %d: create failed, retrying: %v\n", attempt, err)
		time.Sleep(2 * time.Second)
	}
	return errors.New("failed to create cloud topic foo")
}

// parallel_driver_produce: produce a bounded random batch. Best-effort under
// fault injection — transient failures are expected and not bugs.
func produce() error {
	// A per-invocation nonce keeps this producer's keys distinct from every
	// other concurrent producer's, so (nonce, seq) uniquely identifies a
	// record. It also rides in the ClientID so Redpanda's request logs can be
	// correlated back to this batch.
	nonce := rng.Uint64()
	cl, err := newClient(
		kgo.ClientID(fmt.Sprintf("ct_stress/produce/%016x", nonce)),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		// Assign partitions ourselves so each record can carry the partition
		// it was written to; a reader then verifies it was served from there.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	count := 1 + randN(50)
	recs := make([]*kgo.Record, count)
	for i := range recs {
		recs[i] = makeRecord(nonce, i, int32(randN(fooPartitions)))
	}

	fmt.Printf("producing %d records to foo (nonce=%016x)\n", count, nonce)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		fmt.Printf("produce did not complete (expected under faults) (nonce=%016x): %v\n", nonce, err)
		return nil
	}
	// Prove the workload actually writes data in some timeline; without this a
	// run where produce never succeeds would pass every safety check vacuously,
	// since the checkers only assert on non-empty reads.
	assert.Reachable("workload produced a batch to foo",
		map[string]any{"count": count, "nonce": fmt.Sprintf("%016x", nonce)})
	fmt.Printf("produced %d records to foo (nonce=%016x)\n", count, nonce)
	return nil
}
