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

	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newPurgeCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "purge",
		Short: "Stop and remove an existing local container cluster's data",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()
			return common.WrapIfConnErr(purgeCluster(c))
		},
	}

	return command
}

func purgeCluster(c common.Client) error {
	nodes, err := common.GetExistingNodes(c)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		fmt.Print("No nodes to remove.\nYou may start a new local cluster with 'rpk container start'\n")
		return nil
	}
	err = stopCluster(c)
	if err != nil {
		return err
	}
	grp, _ := errgroup.WithContext(context.Background())
	for _, node := range nodes {
		id := node.ID
		var mu sync.Mutex
		printf := func(msg string, args ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Printf(msg+"\n", args...)
		}
		grp.Go(func() error {
			ctx, _ := common.DefaultCtx()
			name := common.Name(id)
			err := c.ContainerRemove(
				ctx,
				name,
				types.ContainerRemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				},
			)
			if err != nil {
				if !c.IsErrNotFound(err) {
					return err
				}
				printf("Unable to remove container %s (node %d)", name, id, err)
			} else {
				printf("Removed container %s (node %d)", name, id)
			}
			return nil
		})
	}
	err = grp.Wait()
	if err != nil {
		return err
	}
	err = common.RemoveNetwork(c)
	if err != nil {
		return err
	}
	fmt.Println("Deleted cluster data.")
	return nil
}
