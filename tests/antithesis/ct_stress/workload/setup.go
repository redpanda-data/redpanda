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
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/twmb/franz-go/pkg/kadm"
	"golang.org/x/sys/unix"
)

// expectedBrokers is the broker count the workload waits for, from the
// EXPECTED_BROKERS env var (set by the compose template), defaulting to 1.
func expectedBrokers() int {
	if v := os.Getenv("EXPECTED_BROKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 1
}

// waitClusterReady blocks until the cluster serves metadata with at least
// `expected` brokers and an elected controller, or `timeout` elapses. Used both
// at startup and by the finally check while the cluster recovers after faults.
func waitClusterReady(expected int, timeout time.Duration) error {
	cl, err := newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		md, err := adm.Metadata(ctx)
		cancel()
		if err == nil && len(md.Brokers) >= expected && md.Controller >= 0 {
			fmt.Printf("cluster ready: %d brokers, controller %d\n", len(md.Brokers), md.Controller)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster not ready after timeout (brokers=%d want=%d)", len(md.Brokers), expected)
		}
		fmt.Printf("waiting for cluster: %d/%d brokers\n", len(md.Brokers), expected)
		time.Sleep(2 * time.Second)
	}
}

// setup is the workload container's entrypoint (not a test command): it waits
// until the cluster serves metadata with the expected brokers, emits the
// Antithesis setup_complete signal, then idles so Antithesis can run the test
// commands in this container. setup_complete must come from a long-lived
// process here, never from a test command.
func setup() error {
	expected := expectedBrokers()
	if err := waitClusterReady(expected, 5*time.Minute); err != nil {
		return err
	}

	lifecycle.SetupComplete(map[string]any{"brokers": expected})
	fmt.Println("emitted setup_complete; workload container idle")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, unix.SIGINT, unix.SIGTERM)
	<-sig
	return nil
}
