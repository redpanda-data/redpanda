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
	"sync"
	"time"
	"vectorized/pkg/cli/cmd/container/common"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func Stop(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "stop",
		Short: "Stop an existing local container cluster",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()
			return common.WrapIfConnErr(stopCluster(fs, c))
		},
	}
	return command
}

func stopCluster(fs afero.Fs, c common.Client) error {
	nodeIDs, err := common.GetExistingNodes(fs)
	if err != nil {
		return err
	}
	if len(nodeIDs) == 0 {
		log.Info(
			`No cluster available.
You may start a new cluster with 'rpk container start'`,
		)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(nodeIDs))
	for _, nodeID := range nodeIDs {
		go func(id uint) {
			defer wg.Done()
			state, err := common.GetState(c, id)
			if err != nil {
				log.Errorf("Couldn't get node %d's state", id)
				return
			}
			// If the node was stopped already, do nothing.
			if !state.Running {
				log.Infof("Node %d was stopped already.", id)
				return
			}
			log.Infof("Stopping node %d", id)
			ctx := context.Background()
			// Redpanda sometimes takes a while to stop, so 20
			// seconds is a safe estimate
			timeout := 20 * time.Second
			err = c.ContainerStop(
				ctx,
				common.Name(id),
				&timeout,
			)
			if err != nil {
				log.Errorf("Couldn't stop node %d", id)
				log.Debug(err)
				return
			}
		}(nodeID)
	}
	wg.Wait()
	return nil
}
