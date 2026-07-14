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

const topicPropertyCleanupPolicy = "cleanup.policy"
const topicPropertyStorageMode = "redpanda.storage.mode"

const storageModeCloud = "cloud"
const storageModeTiered = "tiered"

var storageModes = []string{storageModeCloud, storageModeTiered}

// first_create_topic: create the topics the workload exercises. Runs once
// per timeline after setup_complete; must not signal lifecycle itself.
//
// Each topic's storage mode is an independent choice between cloud and
// tiered_cloud, drawn with the SDK's RandomChoice so Antithesis knows a
// structured random decision happens here and can steer it to explore the
// mode combinations deliberately.
func createTestTopics() error {
	cl, err := newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	fooMode := random.RandomChoice(storageModes)
	ctcMode := random.RandomChoice(storageModes)
	fmt.Printf("randomly chosen storage modes: foo=%s ctc=%s\n", fooMode, ctcMode)

	fooCfg := map[string]*string{topicPropertyStorageMode: &fooMode}
	if err := createOneTopic(adm, topic, fooPartitions, fooReplicas, fooCfg); err != nil {
		return err
	}

	ctcCfg := map[string]*string{
		topicPropertyStorageMode:   &ctcMode,
		topicPropertyCleanupPolicy: new("compact"),
	}
	if err := createOneTopic(adm, ctcTopic, ctcPartitions, ctcReplicas, ctcCfg); err != nil {
		return err
	}

	return nil
}

func createOneTopic(adm *kadm.Client, name string, partitions int32, replicas int16, cfg map[string]*string) error {
	modeForLog := "unset"
	if m := cfg[topicPropertyStorageMode]; m != nil {
		modeForLog = *m
	}

	ctx := context.Background()
	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := adm.CreateTopics(ctx, partitions, replicas, cfg, name)
		if err == nil {
			if terr := resp[name].Err; terr == nil {
				fmt.Printf("created topic %s (%d partitions, %d replicas, mode=%s)\n",
					name, partitions, replicas, modeForLog)
				return nil
			} else if errors.Is(terr, kerr.TopicAlreadyExists) {
				fmt.Printf("topic %s already exists\n", name)
				return nil
			} else {
				err = terr
			}
		}
		fmt.Printf("attempt %d: create %s failed, retrying: %v\n", attempt, name, err)
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("failed to create topic %s", name)
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
		// Flag franz-go's data-loss detection: it otherwise resets the
		// producer id and silently carries on, hiding an anomaly we want seen.
		kgo.ProducerOnDataLossDetected(reportDataLoss),
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

// reportDataLoss is the franz-go ProducerOnDataLossDetected hook. franz-go
// calls it when a produce response returns an out-of-order sequence number or
// unknown producer id that it cannot attribute to benign prefix truncation
// (the broker's log start offset moving past records we'd already had acked).
// franz-go treats that as data loss and, since we don't stop the producer,
// resets the producer id and sequence numbers and continues. It is a
// presumption, not proof — a producer-id expiry can trigger it too — but on an
// acks=all cloud-topic producer it is an anomaly worth surfacing, so record it
// as a reachability failure for Antithesis.
func reportDataLoss(topic string, part int32) {
	details := map[string]any{"topic": topic, "partition": part}
	assert.Unreachable("idempotent producer detected data loss (out-of-order sequence or unknown producer id)", details)
	fmt.Printf("data loss detected on %s/%d (out-of-order sequence or unknown producer id)\n", topic, part)
}
