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
	cl, err := newClient(
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	count := 1 + randN(50)
	recs := make([]*kgo.Record, count)
	for i := range recs {
		recs[i] = &kgo.Record{
			Key:   fmt.Appendf(nil, "k%d-%d", randN(1<<30), i),
			Value: fmt.Appendf(nil, "val-%d-%d", randN(1<<30), i),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		fmt.Printf("produce did not complete (expected under faults): %v\n", err)
		return nil
	}
	fmt.Printf("produced %d records to foo\n", count)
	return nil
}

// parallel_driver_consume: consume a random offset range from a random
// partition, exercising the cloud-topics read path. Best-effort.
func consume() error {
	part, lo, hi, err := randomPartitionRange()
	if err != nil || hi <= lo {
		return nil // unreadable under faults, or empty; not a bug
	}
	o1 := lo + int64(randN(int(hi-lo)))
	o2 := o1 + 1 + int64(randN(int(hi-o1)))

	offs, err := readRange(part, o1, o2)
	if err != nil {
		return nil
	}
	fmt.Printf("consumed foo/%d offsets %d:%d -> %d records\n", part, o1, o2, len(offs))
	return nil
}
