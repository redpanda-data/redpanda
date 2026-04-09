// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package container

import (
	"context"
	"fmt"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/containerutil"
	"github.com/spf13/cobra"
)

func newStopCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "stop",
		Short: "Stop an existing local container cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := containerutil.NewDockerClient(cmd.Context())
			if err != nil {
				return err
			}
			defer c.Close()
			return containerutil.WrapIfConnErr(stopCluster(cmd.Context(), c))
		},
	}
	return command
}

func stopCluster(ctx context.Context, c containerutil.Client) error {
	nodes, err := containerutil.GetExistingNodes(ctx, c)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		fmt.Print("No cluster available.\nYou may start a new cluster with 'rpk container start'\n")
	}

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	for _, node := range nodes {
		var mu sync.Mutex
		printf := func(msg string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Printf(msg+"\n", args...)
		}
		go func(state *containerutil.NodeState) {
			defer wg.Done()
			name := containerutil.RedpandaName(state.ID)
			if state.Console {
				name = containerutil.ConsoleContainerName
			}
			// If the node was stopped already, do nothing.
			if !state.Running {
				printf("%s was stopped already.", name)
				return
			}
			// Redpanda sometimes takes a while to stop, so 20
			// seconds is a safe estimate
			timeout := 20 // seconds

			printf("Stopping %s", name)
			err := c.ContainerStop(ctx, name, container.StopOptions{Timeout: &timeout})
			if err != nil {
				printf("Unable to stop node %d: %v", state.ID, err)
				return
			}
		}(node)
	}
	wg.Wait()
	return nil
}
