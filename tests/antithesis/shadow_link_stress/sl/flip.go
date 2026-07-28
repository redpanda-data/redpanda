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
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/twmb/franz-go/pkg/kadm"
)

const topicPropertyStorageMode = "redpanda.storage.mode"

const (
	storageModeCloud  = "cloud"
	storageModeTiered = "tiered"
)

// flippableTopics returns the test topics whose storage mode the flip driver
// toggles: the ones created with redpanda.storage.mode set. local is excluded
// by construction — it is the no-remote-storage control topic.
func flippableTopics() []testTopic {
	var out []testTopic
	for _, t := range testTopics {
		if t.config[topicPropertyStorageMode] != nil {
			out = append(out, t)
		}
	}
	return out
}

// flipTargetMode maps a topic's current storage mode to the mode the flip
// driver alters it to; ok is false for anything but the two cloud-backed
// modes (which nothing in this workload ever sets, so anything else is an
// anomaly).
func flipTargetMode(mode string) (target string, ok bool) {
	switch mode {
	case storageModeCloud:
		return storageModeTiered, true
	case storageModeTiered:
		return storageModeCloud, true
	default:
		return "", false
	}
}

// describeStorageMode reads topic t's redpanda.storage.mode on cluster c.
// The property reports plain "cloud" or "tiered" (the tiered impl resolves
// via the cluster default, pinned to tiered_v2 on both clusters).
func describeStorageMode(c *cluster, t string) (string, error) {
	cl, err := c.newClient()
	if err != nil {
		return "", err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rcs, err := adm.DescribeTopicConfigs(ctx, t)
	if err != nil {
		return "", err
	}
	rc, err := rcs.On(t, nil)
	if err == nil && rc.Err != nil {
		err = rc.Err
	}
	if err != nil {
		return "", err
	}
	for _, cfg := range rc.Configs {
		if cfg.Key == topicPropertyStorageMode {
			return cfg.MaybeValue(), nil
		}
	}
	return "", nil
}

// parallel_driver_flip_storage_mode: toggle a random remote-storage test
// topic between the two cloud-backed storage modes (cloud <-> tiered) on the
// SOURCE while replication continues. The target is never altered directly:
// the cluster_link reconciler propagates source mode updates to the shadow
// topic, and that propagation racing shadow fetch, faults and replica moves
// is the surface this driver exists to exercise (the eventually check
// asserts the modes converge once faults stop). Best-effort under fault
// injection: a failed describe or a rejected alter is expected and not a bug.
func flipStorageMode() error {
	t := random.RandomChoice(flippableTopics()).name

	mode, err := describeStorageMode(srcCluster, t)
	if err != nil {
		fmt.Printf("flip %s skipped: describe configs failed (expected under faults): %v\n", t, err)
		return nil
	}
	target, ok := flipTargetMode(mode)
	if !ok {
		// The topics are created in one of the two modes and only this
		// driver alters the mode, so anything else is a real anomaly.
		assert.Unreachable("shadow-linked topic reports an unexpected storage mode",
			map[string]any{"topic": t, "mode": mode})
		return nil
	}

	cl, err := srcCluster.newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resps, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{{
		Op:    kadm.SetConfig,
		Name:  topicPropertyStorageMode,
		Value: &target,
	}}, t)
	accepted := false
	if err == nil {
		resp, rerr := resps.On(t, nil)
		if rerr == nil && resp.Err == nil {
			accepted = true
		} else if rerr != nil {
			err = rerr
		} else {
			err = fmt.Errorf("%w (%s)", resp.Err, resp.ErrMessage)
		}
	}

	// One liveness property per topic and direction, so a run only passes
	// this set once every transition has been accepted on every topic
	// somewhere.
	details := map[string]any{"topic": t, "from": mode, "to": target}
	toTiered := target == storageModeTiered
	switch t {
	case "tsv2":
		if toTiered {
			assert.Sometimes(accepted, "source topic tsv2 flip cloud -> tiered accepted", details)
		} else {
			assert.Sometimes(accepted, "source topic tsv2 flip tiered -> cloud accepted", details)
		}
	case "cloud":
		if toTiered {
			assert.Sometimes(accepted, "source topic cloud flip cloud -> tiered accepted", details)
		} else {
			assert.Sometimes(accepted, "source topic cloud flip tiered -> cloud accepted", details)
		}
	}

	if accepted {
		fmt.Printf("flipped %s storage mode %s -> %s on source\n", t, mode, target)
	} else {
		fmt.Printf("flip %s %s -> %s not accepted (expected under faults): %v\n", t, mode, target, err)
	}
	return nil
}
