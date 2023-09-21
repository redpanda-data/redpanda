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
	"os"
	"sync"

	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newPurgeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	command := &cobra.Command{
		Use:   "purge",
		Short: "Stop and remove an existing local container cluster's data",
		RunE: func(*cobra.Command, []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()
			purged, err := purgeCluster(c)
			if err != nil {
				return common.WrapIfConnErr(err)
			}
			if !purged {
				return nil
			}
			cfg, err := p.Load(fs)
			if err != nil {
				return fmt.Errorf("unable to load config: %v", err)
			}
			y, ok := cfg.ActualRpkYaml()
			if !ok || y.Profile(containerProfileName) == nil {
				// rpk.yaml file nor profile exist, we exit.
				return nil
			}
			cleared, err := profile.DeleteProfile(fs, y, containerProfileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "unable to delete %q profile: %v; you may delete the profile manually running 'rpk profile delete %v'", containerProfileName, err, containerProfileName)
			}
			fmt.Printf("Deleted %q profile.\n", containerProfileName)
			if cleared {
				fmt.Println("This was the selected profile; rpk will use defaults until a new profile is selected or a new container is created.")
			}
			return nil
		},
	}

	return command
}

func purgeCluster(c common.Client) (purged bool, rerr error) {
	nodes, err := common.GetExistingNodes(c)
	if err != nil {
		return false, err
	}
	if len(nodes) == 0 {
		fmt.Print("No nodes to remove.\nYou may start a new local cluster with 'rpk container start'\n")
		return false, nil
	}
	err = stopCluster(c)
	if err != nil {
		return false, err
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
		return false, err
	}
	err = common.RemoveNetwork(c)
	if err != nil {
		return false, err
	}
	fmt.Println("Deleted cluster data.")
	return true, nil
}
