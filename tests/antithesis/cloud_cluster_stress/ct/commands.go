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

const topicPropertyStorageMode = "redpanda.storage.mode"

const storageModeCloud = "cloud"
const storageModeTiered = "tiered"

var storageModes = []string{storageModeCloud, storageModeTiered}

// first_create_topics: create the topics the workload exercises. Runs once
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

	for _, t := range testTopics {
		mode := random.RandomChoice(storageModes)
		fmt.Printf("randomly chosen storage mode: %s=%s\n", t.name, mode)
		cfg := map[string]*string{topicPropertyStorageMode: &mode}
		if err := createOneTopic(adm, t.name, t.partitions, t.replicas, cfg); err != nil {
			return err
		}
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
