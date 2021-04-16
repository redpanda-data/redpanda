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
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/docker/docker/api/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/net"
	"golang.org/x/sync/errgroup"
)

type node struct {
	id   uint
	addr string
}

func Start() *cobra.Command {
	var (
		nodes   uint
		retries uint
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
				checkBrokers,
				retries,
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

	command.Flags().UintVar(
		&retries,
		"retries",
		10,
		"The amount of times to check for the cluster before"+
			" considering it unstable and exiting.",
	)

	return command
}

func startCluster(
	c common.Client, n uint, check func([]node) func() error, retries uint,
) error {
	// Check if cluster exists and start it again.
	restarted, err := restartCluster(c, check, retries)
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
	seedProxyPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedRPCPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedMetricsPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedState, err := common.CreateNode(
		c,
		seedID,
		seedKafkaPort,
		seedProxyPort,
		seedRPCPort,
		seedMetricsPort,
		netID,
	)
	if err != nil {
		return err
	}

	log.Info("Starting cluster")
	err = startNode(
		c,
		seedState.ContainerID,
	)
	if err != nil {
		return err
	}

	seedNode := node{
		seedID,
		nodeAddr(seedKafkaPort),
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
			proxyPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			rpcPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			metricsPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			args := []string{
				"--seeds",
				fmt.Sprintf(
					"%s:%d",
					seedState.ContainerIP,
					config.Default().Redpanda.RPCServer.Port,
				),
			}
			state, err := common.CreateNode(
				c,
				id,
				kafkaPort,
				proxyPort,
				rpcPort,
				metricsPort,
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
				state.ContainerID,
			)
			if err != nil {
				return err
			}
			mu.Lock()
			nodes = append(nodes, node{
				id,
				nodeAddr(state.HostKafkaPort),
			})
			mu.Unlock()
			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return fmt.Errorf("Error restarting the cluster: %v", err)
	}
	err = waitForCluster(check(nodes), retries)
	if err != nil {
		return err
	}
	renderClusterInfo(nodes)
	log.Infof(
		"\nCluster started! You may use rpk to interact with it." +
			" E.g:\n\nrpk cluster info\n",
	)

	return nil
}

func restartCluster(
	c common.Client, check func([]node) func() error, retries uint,
) ([]node, error) {
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
				nodeAddr(state.HostKafkaPort),
			})
			mu.Unlock()
			return nil
		})
	}
	err = grp.Wait()
	if err != nil {
		return nil, fmt.Errorf("Error restarting the cluster: %v", err)
	}
	err = waitForCluster(check(nodes), retries)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func startNode(c common.Client, containerID string) error {
	ctx, _ := common.DefaultCtx()
	err := c.ContainerStart(ctx, containerID, types.ContainerStartOptions{})
	return err
}

func checkBrokers(nodes []node) func() error {
	return func() error {
		addrs := make([]string, 0, len(nodes))
		for _, n := range nodes {
			addrs = append(addrs, n.addr)
		}
		client, err := kafka.InitClient(addrs...)
		if err != nil {
			return err
		}
		lenBrokers := len(client.Brokers())
		if lenBrokers != len(nodes) {
			return fmt.Errorf(
				"Expected %d nodes, got %d.",
				len(nodes),
				lenBrokers,
			)
		}
		return nil
	}
}

func waitForCluster(check func() error, retries uint) error {
	log.Info("Waiting for the cluster to be ready...")
	return retry.Do(
		check,
		retry.Attempts(retries),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			log.Debugf("Cluster didn't stabilize: %v", err)
			log.Debugf("Retrying (%d retries left)", retries-n)
		}),
	)
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

func nodeAddr(port uint) string {
	return fmt.Sprintf(
		"127.0.0.1:%d",
		port,
	)
}
