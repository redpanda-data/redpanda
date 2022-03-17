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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func Stop() *cobra.Command {
	command := &cobra.Command{
		Use:   "stop",
		Short: "Stop an existing local container cluster.",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()
			return common.WrapIfConnErr(stopCluster(c))
		},
	}
	return command
}

func stopCluster(c common.Client) error {
	nodes, err := common.GetExistingNodes(c)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		log.Info(
			`No cluster available.
You may start a new cluster with 'rpk container start'`,
		)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(state *common.NodeState) {
			defer wg.Done()
			// If the node was stopped already, do nothing.
			if !state.Running {
				log.Infof(
					"Node %d was stopped already.",
					state.ID,
				)
				return
			}
			log.Infof("Stopping node %d", state.ID)
			ctx := context.Background()
			// Redpanda sometimes takes a while to stop, so 20
			// seconds is a safe estimate
			timeout := 20 * time.Second
			err = c.ContainerStop(
				ctx,
				common.Name(state.ID),
				&timeout,
			)
			if err != nil {
				log.Errorf("Couldn't stop node %d", state.ID)
				log.Debug(err)
				return
			}
		}(node)
	}
	wg.Wait()
	return nil
}
