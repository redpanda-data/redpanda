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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	vnet "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
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

func newStartCommand() *cobra.Command {
	var (
		nodes   uint
		retries uint
		image   string
	)
	command := &cobra.Command{
		Use:   "start",
		Short: "Start a local container cluster",
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
		fmt.Print("Found an existing cluster:\n\n")
		renderClusterInfo(c)
		if len(restarted) != int(n) {
			fmt.Print("\nTo change the number of nodes, first purge the existing cluster with\n'rpk container purge'.\n\n")
		}
		return nil
	}

	fmt.Println("Checking for a local image...")
	present, checkErr := common.CheckIfImgPresent(c, image)
	if checkErr != nil {
		fmt.Printf("Error trying to list local images: %v\n", err)
	}
	if !present {
		// If the image isn't present locally, try to pull it.
		fmt.Println("Downloading latest version of redpanda")
		err = common.PullImage(c, image)
		if err != nil {
			return err
		}
	}

	// Create the docker network if it doesn't exist already
	netID, err := common.CreateNetwork(c)
	if err != nil {
		return err
	}

	reqPorts := n * 5 // we need 5 ports per node
	ports, err := vnet.GetFreePortPool(int(reqPorts))
	if err != nil {
		return err
	}

	// Start a seed node.
	var (
		seedID            uint
		seedKafkaPort     = ports[0]
		seedProxyPort     = ports[1]
		seedSchemaRegPort = ports[2]
		seedRPCPort       = ports[3]
		seedMetricsPort   = ports[4]
	)

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

	fmt.Println("Starting cluster...")
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
			var (
				kafkaPort     = ports[0+5*id]
				proxyPort     = ports[1+5*id]
				schemaRegPort = ports[2+5*id]
				rpcPort       = ports[3+5*id]
				metricsPort   = ports[4+5*id]
			)

			args := []string{
				"--seeds",
				net.JoinHostPort(
					seedState.ContainerIP,
					strconv.Itoa(config.DevDefault().Redpanda.RPCServer.Port),
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
		return fmt.Errorf("error restarting the cluster: %v", err)
	}
	err = waitForCluster(check(nodes), retries)
	if err != nil {
		return err
	}

	fmt.Println("Cluster started!")
	dockerNodes, err := renderClusterInfo(c)
	if err != nil {
		return err
	}
	renderClusterInteract(dockerNodes)

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
		return nil, fmt.Errorf("error restarting the cluster: %v", err)
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
				"expected %d nodes, got %d",
				len(nodes),
				len(brokers),
			)
		}
		return nil
	}
}

func waitForCluster(check func() error, retries uint) error {
	fmt.Printf("Waiting for the cluster to be ready...\n\n")
	return retry.Do(
		check,
		retry.Attempts(retries),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
}

func renderClusterInfo(c common.Client) ([]*common.NodeState, error) {
	nodes, err := common.GetExistingNodes(c)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		fmt.Println("No Redpanda nodes detected - use `rpk container start` or check `docker ps` if you expected nodes")
		return nil, nil
	}

	tw := out.NewTable("Node-ID", "Status", "Kafka-Address", "Admin-Address", "Proxy-Address")
	defer tw.Flush()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	for _, node := range nodes {
		kafka := nodeAddr(node.HostKafkaPort)
		admin := nodeAddr(node.HostAdminPort)
		proxy := nodeAddr(node.HostProxyPort)
		if node.HostKafkaPort == 0 {
			kafka = "-"
		}
		if node.HostAdminPort == 0 {
			admin = "-"
		}
		if node.HostProxyPort == 0 {
			proxy = "-"
		}
		tw.PrintStrings(
			fmt.Sprint(node.ID),
			node.Status,
			kafka,
			admin,
			proxy,
		)
	}
	return nodes, nil
}

func renderClusterInteract(nodes []*common.NodeState) {
	var (
		brokers    []string
		adminAddrs []string
	)
	for _, node := range nodes {
		if node.Running {
			brokers = append(brokers, nodeAddr(node.HostKafkaPort))
			adminAddrs = append(adminAddrs, nodeAddr(node.HostAdminPort))
		}
	}
	if len(brokers) == 0 || len(adminAddrs) == 0 {
		return
	}

	m := `
You can use rpk to interact with this cluster. E.g:

    rpk cluster info -X brokers=%s
    rpk cluster health -X admin.hosts=%s

You may also set an environment variable with the comma-separated list of
broker and admin API addresses:

    export RPK_BROKERS="%s"
    export RPK_ADMIN_HOSTS="%s"
    rpk cluster info
    rpk cluster health

`
	b := strings.Join(brokers, ",")
	a := strings.Join(adminAddrs, ",")
	fmt.Printf(m, b, a, b, a)
}

func nodeAddr(port uint) string {
	return fmt.Sprintf(
		"127.0.0.1:%d",
		port,
	)
}
