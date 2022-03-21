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
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	vnet "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

type node struct {
	id   uint
	addr string
}

func collectFlags(args []string, flag string) []string {
	flags := []string{}
	i := 0
	for i < len(args)-1 {
		if args[i] == flag {
			flags = append(flags, args[i], args[i+1])
		}
		i++
	}
	return flags
}

func Start() *cobra.Command {
	var (
		nodes   uint
		retries uint
		image   string
	)
	command := &cobra.Command{
		Use:   "start",
		Short: "Start a local container cluster.",
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			// Allow unknown flags so that arbitrary flags can be passed
			// through to the containers without the need to pass '--'
			// (POSIX standard)
			UnknownFlags: true,
		},
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

			configKvs := collectFlags(os.Args, "--set")

			return common.WrapIfConnErr(startCluster(
				c,
				nodes,
				checkBrokers,
				retries,
				image,
				configKvs,
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
	imageFlag := "image"
	command.Flags().StringVar(
		&image,
		imageFlag,
		common.DefaultImage(),
		"An arbitrary container image to use.",
	)
	command.Flags().MarkHidden(imageFlag)

	return command
}

func startCluster(
	c common.Client,
	n uint,
	check func([]node) func() error,
	retries uint,
	image string,
	extraArgs []string,
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

	log.Debug("Checking for a local image.")
	present, checkErr := common.CheckIfImgPresent(c, image)
	if checkErr != nil {
		log.Debugf("Error trying to list local images: %v", err)
	}
	if !present {
		// If the image isn't present locally, try to pull it.
		log.Info("Downloading latest version of Redpanda")
		err = common.PullImage(c, image)
		if err != nil {
			msg := "Couldn't pull image and a local one wasn't found either"
			if c.IsErrConnectionFailed(err) {
				log.Debug(err)
				msg += ".\nPlease check your internet connection" +
					" and try again."
				return errors.New(msg)
			}
			return fmt.Errorf(
				"%s: %v",
				msg,
				err,
			)
		}
	}

	// Create the docker network if it doesn't exist already
	netID, err := common.CreateNetwork(c)
	if err != nil {
		return err
	}

	// Start a seed node.
	seedID := uint(0)
	seedKafkaPort, err := vnet.GetFreePort()
	if err != nil {
		return err
	}
	seedProxyPort, err := vnet.GetFreePort()
	if err != nil {
		return err
	}
	seedSchemaRegPort, err := vnet.GetFreePort()
	if err != nil {
		return err
	}
	seedRPCPort, err := vnet.GetFreePort()
	if err != nil {
		return err
	}
	seedMetricsPort, err := vnet.GetFreePort()
	if err != nil {
		return err
	}
	seedState, err := common.CreateNode(
		c,
		seedID,
		seedKafkaPort,
		seedProxyPort,
		seedSchemaRegPort,
		seedRPCPort,
		seedMetricsPort,
		netID,
		image,
		extraArgs...,
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
			kafkaPort, err := vnet.GetFreePort()
			if err != nil {
				return err
			}
			proxyPort, err := vnet.GetFreePort()
			if err != nil {
				return err
			}
			schemaRegPort, err := vnet.GetFreePort()
			if err != nil {
				return err
			}
			rpcPort, err := vnet.GetFreePort()
			if err != nil {
				return err
			}
			metricsPort, err := vnet.GetFreePort()
			if err != nil {
				return err
			}
			args := []string{
				"--seeds",
				net.JoinHostPort(
					seedState.ContainerIP,
					strconv.Itoa(config.Default().Redpanda.RPCServer.Port),
				),
			}
			state, err := common.CreateNode(
				c,
				id,
				kafkaPort,
				proxyPort,
				schemaRegPort,
				rpcPort,
				metricsPort,
				netID,
				image,
				append(args, extraArgs...)...,
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
	var brokers []string
	for _, node := range nodes {
		brokers = append(brokers, node.addr)
	}
	log.Infof(
		"\nCluster started! You may use rpk to interact with it."+
			" E.g:\n\nrpk cluster info --brokers %s\n", strings.Join(brokers, ","),
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
		cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
		if err != nil {
			return err
		}
		brokers, err := kadm.NewClient(cl).ListBrokers(context.Background())
		if err != nil {
			return err
		}
		if len(brokers) != len(nodes) {
			return fmt.Errorf(
				"Expected %d nodes, got %d.",
				len(nodes),
				len(brokers),
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
