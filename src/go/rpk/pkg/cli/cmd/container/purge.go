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

	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/docker"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/podman"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newPurgeCommand() *cobra.Command {
	var usepodman bool

	command := &cobra.Command{
		Use:   "purge",
		Short: "Stop and remove an existing local container cluster's data",
		RunE: func(_ *cobra.Command, _ []string) error {
			var cli common.GenericClient
			if usepodman {
				cli = &podman.PodmanClient{}
			} else {
				cli = &docker.DockerClient{}
			}
			cli.SetConnection()
			defer cli.Close()
			return common.WrapIfConnErr(purgeCluster(cli))
		},
	}

	command.Flags().BoolVar(
		&usepodman,
		"podman",
		false,
		"Use podman instead of docker (default: docker)",
	)

	return command
}

func purgeCluster(c common.GenericClient) error {
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
