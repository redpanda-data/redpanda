// Copyright 2020 Vectorized, Inc.
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

	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func Purge() *cobra.Command {
	command := &cobra.Command{
		Use:   "purge",
		Short: "Stop and remove an existing local container cluster's data.",
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
		log.Info(
			`No nodes to remove.
You may start a new local cluster with 'rpk container start'`,
		)
		return nil
	}
	err = stopCluster(c)
	if err != nil {
		return err
	}
	grp, _ := errgroup.WithContext(context.Background())
	for _, node := range nodes {
		id := node.ID
		grp.Go(func() error {
			ctx, _ := common.DefaultCtx()
			name := common.Name(id)
			err = c.ContainerRemove(
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
				log.Debug(err)
			}
			log.Debugf("Removed node '%d'", id)
			log.Debugf("Removed container '%s'", name)
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
	log.Info("Deleted cluster data.")
	return nil
}
