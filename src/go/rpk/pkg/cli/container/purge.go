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

	"github.com/docker/docker/api/types/container"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newPurgeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	command := &cobra.Command{
		Use:   "purge",
		Short: "Stop and remove an existing local container cluster's data",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			c, err := common.NewDockerClient(cmd.Context())
			out.MaybeDie(err, "unable to create docker client: %v", err)
			defer c.Close()

			purged, err := purgeCluster(c)
			out.MaybeDieErr(common.WrapIfConnErr(err))

			if !purged {
				return
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok || y.Profile(common.ContainerProfileName) == nil {
				// rpk.yaml file nor profile exist, we exit.
				return
			}
			cleared, err := profile.DeleteProfile(fs, y, common.ContainerProfileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "unable to delete %q profile: %v; you may delete the profile manually running 'rpk profile delete %v'", common.ContainerProfileName, err, common.ContainerProfileName)
				return
			}
			fmt.Printf("Deleted profile %q.\n", common.ContainerProfileName)
			if cleared {
				fmt.Println("This was the selected profile; rpk will use defaults until a new profile is selected or a new container is created.")
			}
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
		node := node
		id := node.ID
		var mu sync.Mutex
		printf := func(msg string, args ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			fmt.Printf(msg+"\n", args...)
		}
		grp.Go(func() error {
			ctx, _ := common.DefaultCtx()
			name := common.RedpandaName(id)
			if node.Console {
				name = common.ConsoleContainerName
			}
			err := c.ContainerRemove(
				ctx,
				name,
				container.RemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				},
			)
			if err != nil {
				if !c.IsErrNotFound(err) {
					return err
				}
				printf("Unable to remove container %s", name, err)
			} else {
				printf("Removed container %s", name)
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
