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

// parallel_driver_flip_storage_mode: toggle a random test topic between the
// two cloud-backed storage modes (cloud <-> tiered_v2) while the rest of the
// workload keeps producing, consuming and moving replicas. Antithesis
// scheduling this driver at arbitrary points is what exercises mid-flight
// transitions.
//
// The flip reads the topic's current mode from redpanda.storage.mode (which
// reports plain "cloud" or "tiered") and alters it to the other one. Altering
// to "tiered" selects tiered_v2 only because the cluster bootstraps with
// default_redpanda_storage_mode_tiered_impl=tiered_v2 (cloud -> tiered_v1 is
// a forbidden transition). Best-effort under fault injection: a failed
// describe or a rejected alter is expected and not a bug.
func flipStorageMode() error {
	t := random.RandomChoice(testTopics).name

	cl, err := newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rcs, err := adm.DescribeTopicConfigs(ctx, t)
	if err != nil {
		fmt.Printf("flip %s skipped: describe configs failed (expected under faults): %v\n", t, err)
		return nil
	}
	rc, err := rcs.On(t, nil)
	if err == nil && rc.Err != nil {
		err = rc.Err
	}
	if err != nil {
		fmt.Printf("flip %s skipped: describe configs failed (expected under faults): %v\n", t, err)
		return nil
	}

	mode := ""
	for _, c := range rc.Configs {
		if c.Key == topicPropertyStorageMode {
			mode = c.MaybeValue()
		}
	}
	var target string
	switch mode {
	case storageModeCloud:
		target = storageModeTiered
	case storageModeTiered:
		target = storageModeCloud
	default:
		// The topics are created in one of the two modes above and only this
		// driver alters the mode, so anything else is a real anomaly.
		assert.Unreachable("cloud topic reports an unexpected storage mode",
			map[string]any{"topic": t, "mode": mode})
		return nil
	}

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

	// One liveness property per direction, so a run only passes this pair
	// once both transitions have actually been accepted somewhere.
	details := map[string]any{"topic": t, "from": mode, "to": target}
	if target == storageModeTiered {
		assert.Sometimes(accepted, "storage mode flip cloud -> tiered accepted", details)
	} else {
		assert.Sometimes(accepted, "storage mode flip tiered -> cloud accepted", details)
	}

	if accepted {
		fmt.Printf("flipped %s storage mode %s -> %s\n", t, mode, target)
	} else {
		fmt.Printf("flip %s %s -> %s not accepted (expected under faults): %v\n", t, mode, target, err)
	}
	return nil
}
