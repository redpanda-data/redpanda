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
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/twmb/franz-go/pkg/kadm"
)

// waitClusterReady blocks until cluster c serves metadata with at least its
// expected broker count and an elected controller, or timeout elapses. Used
// at startup and by the eventually check while the clusters recover.
func waitClusterReady(c *cluster, timeout time.Duration) error {
	cl, err := c.newClient()
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	expected := c.expected()
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		md, err := adm.Metadata(ctx)
		cancel()
		if err == nil && len(md.Brokers) >= expected && md.Controller >= 0 {
			fmt.Printf("%s cluster ready: %d brokers, controller %d\n", c.role, len(md.Brokers), md.Controller)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s cluster not ready after timeout (brokers=%d want=%d, last err=%v)",
				c.role, len(md.Brokers), expected, err)
		}
		fmt.Printf("waiting for %s cluster: %d/%d brokers (err=%v)\n", c.role, len(md.Brokers), expected, err)
		time.Sleep(2 * time.Second)
	}
}

// setup is the workload container's entrypoint (not a test command): it
// waits until both clusters serve metadata with the expected brokers, emits
// the Antithesis setup_complete signal, then idles so Antithesis can run the
// test commands in this container. setup_complete must come from a
// long-lived process here, never from a test command.
func setup() error {
	for _, c := range []*cluster{srcCluster, dstCluster} {
		if err := waitClusterReady(c, 5*time.Minute); err != nil {
			return err
		}
	}

	lifecycle.SetupComplete(map[string]any{
		"source_brokers": srcCluster.expected(),
		"target_brokers": dstCluster.expected(),
	})
	fmt.Println("emitted setup_complete; workload container idle")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	return nil
}
