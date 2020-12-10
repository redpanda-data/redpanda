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
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"vectorized/pkg/cli/cmd/container/common"
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/config"
	"vectorized/pkg/net"

	"github.com/docker/docker/api/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type node struct {
	id   uint
	addr string
}

func Start() *cobra.Command {
	var (
		nodes uint
	)
	command := &cobra.Command{
		Use:   "start",
		Short: "Start a local container cluster",
		RunE: func(_ *cobra.Command, _ []string) error {
			if nodes < 1 {
				return errors.New(
					"--nodes should be 1 or greater",
				)
			}
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()

			return common.WrapIfConnErr(startCluster(
				c,
				nodes,
			))
		},
	}

	command.Flags().UintVarP(
		&nodes,
		"nodes",
		"n",
		1,
		"The number of nodes to start",
	)

	return command
}

func startCluster(c common.Client, n uint) error {
	// Check if cluster exists and start it again.
	restarted, err := restartCluster(c)
	if err != nil {
		return err
	}
	// If a cluster was restarted, there's nothing else to do.
	if len(restarted) != 0 {
		log.Info("\nFound an existing cluster:\n")
		renderClusterInfo(restarted)
		if len(restarted) != int(n) {
			log.Infof(
				"\nTo change the number of nodes, first purge" +
					" the existing cluster:\n\n" +
					"rpk container purge\n",
			)
		}
		return nil
	}

	log.Info("Downloading latest version of Redpanda")
	err = common.PullImage(c)
	if err != nil {
		log.Debugf("Error trying to pull latest image: %v", err)

		msg := "Couldn't pull image and a local one wasn't found either."
		if c.IsErrConnectionFailed(err) {
			msg += "\nPlease check your internet connection" +
				" and try again."
		}

		present, checkErr := common.CheckIfImgPresent(c)

		if checkErr != nil {
			log.Debugf("Error trying to list local images: %v", err)

		}
		if !present {
			return errors.New(msg)
		}
	}

	// Create the docker network if it doesn't exist already
	netID, err := common.CreateNetwork(c)
	if err != nil {
		return err
	}

	// Start a seed node.
	seedID := uint(0)
	seedKafkaPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedRPCPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedState, err := common.CreateNode(
		c,
		seedID,
		seedKafkaPort,
		seedRPCPort,
		netID,
	)
	if err != nil {
		return err
	}

	coreCount := int(math.Max(1, float64(runtime.NumCPU())/float64(n)))

	log.Info("Starting cluster")
	err = startNode(
		c,
		seedID,
		seedKafkaPort,
		seedRPCPort,
		seedID,
		seedState.ContainerID,
		seedState.ContainerIP,
		"",
		coreCount,
	)
	if err != nil {
		return err
	}

	seedNode := node{
		seedID,
		fmt.Sprintf("%s:%d", seedState.ContainerIP, seedKafkaPort),
	}

	nodes := []node{seedNode}

	mu := sync.Mutex{}

	grp, _ := errgroup.WithContext(context.Background())

	for nodeID := uint(1); nodeID < n; nodeID++ {
		id := nodeID
		grp.Go(func() error {
			kafkaPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			rpcPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			args := []string{
				"--seeds",
				fmt.Sprintf(
					"%s:%d+%d",
					seedState.ContainerIP,
					config.Default().Redpanda.RPCServer.Port,
					seedID,
				),
			}
			state, err := common.CreateNode(
				c,
				id,
				kafkaPort,
				rpcPort,
				netID,
				args...,
			)
			if err != nil {
				return err
			}
			log.Debugf(
				"Created container with NodeID=%d, IP=%s, ID='%s",
				id,
				state.ContainerIP,
				state.ContainerID,
			)
			err = startNode(
				c,
				id,
				kafkaPort,
				rpcPort,
				seedRPCPort,
				state.ContainerID,
				state.ContainerIP,
				seedState.ContainerIP,
				coreCount,
			)
			if err != nil {
				return err
			}
			mu.Lock()
			nodes = append(nodes, node{
				id,
				fmt.Sprintf(
					"%s:%d",
					state.ContainerIP,
					state.HostKafkaPort,
				),
			})
			mu.Unlock()
			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return err
	}
	renderClusterInfo(nodes)
	log.Infof(
		"\nCluster started! You may use 'rpk api' to interact with" +
			" the cluster. E.g:\n\nrpk api status\n",
	)

	return nil
}

func restartCluster(c common.Client) ([]node, error) {
	// Check if a cluster is running
	states, err := common.GetExistingNodes(c)
	if err != nil {
		return nil, err
	}
	// If there isn't an existing cluster, there's nothing to restart.
	if len(states) == 0 {
		return nil, nil
	}
	grp, _ := errgroup.WithContext(context.Background())
	mu := sync.Mutex{}
	nodes := []node{}
	for _, s := range states {
		state := s
		grp.Go(func() error {
			if !state.Running {
				ctx, _ := common.DefaultCtx()
				err = c.ContainerStart(
					ctx,
					state.ContainerID,
					types.ContainerStartOptions{},
				)
				if err != nil {
					return err
				}
				state, err = common.GetState(c, state.ID)
				if err != nil {
					return err
				}
			}
			mu.Lock()
			nodes = append(nodes, node{
				state.ID,
				fmt.Sprintf(
					"%s:%d",
					state.ContainerIP,
					state.HostKafkaPort,
				),
			})
			mu.Unlock()
			return nil
		})
	}
	err = grp.Wait()
	return nodes, err
}

func startNode(
	c common.Client,
	nodeID, kafkaPort, rpcPort, seedRPCPort uint,
	containerID, ip, seedIP string,
	cores int,
) error {
	ctx, _ := common.DefaultCtx()
	err := c.ContainerStart(ctx, containerID, types.ContainerStartOptions{})
	return err
}

func renderClusterInfo(nodes []node) {
	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(true)
	t.SetHeader([]string{"Node ID", "Address"})
	for _, node := range nodes {
		t.Append([]string{
			fmt.Sprint(node.id),
			node.addr,
		})
	}

	t.Render()
}
