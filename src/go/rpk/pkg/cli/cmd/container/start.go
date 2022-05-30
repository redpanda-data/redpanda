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

type ports []uint

func (ps *ports) pop() uint {
	if len(*ps) == 0 {
		return 0
	}
	r := (*ps)[0]
	(*ps) = (*ps)[1:]
	return r
}

type clusterPorts struct {
	reg   ports
	proxy ports
	kafka ports
	admin ports
	rpc   ports
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
	const (
		flagRegPorts   = "schema-registry-ports"
		flagProxyPorts = "proxy-ports"
		flagKafkaPorts = "kafka-ports"
		flagAdminPorts = "admin-ports"
		flagRPCPorts   = "broker-rpc-ports"
	)
	var (
		nodes   uint
		retries uint
		image   string

		regPorts   []string
		proxyPorts []string
		kafkaPorts []string
		adminPorts []string
		rpcPorts   []string
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

			var p clusterPorts
			takenPorts := make(map[uint]struct{})
			for _, assign := range []struct {
				name    string
				src     []string
				dst     *ports
				require bool
			}{
				{flagRegPorts, regPorts, &p.reg, false},
				{flagProxyPorts, proxyPorts, &p.proxy, false},
				{flagKafkaPorts, kafkaPorts, &p.kafka, true}, // we require at least one exposed kafka API port
				{flagAdminPorts, adminPorts, &p.admin, false},
				{flagRPCPorts, rpcPorts, &p.rpc, false},
			} {
				var err error
				*assign.dst, err = assignPorts(assign.name, assign.src, takenPorts, nodes, assign.require)
				if err != nil {
					return err
				}
			}

			if err := p.assignAny(takenPorts); err != nil {
				return err
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
				p,
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
		"The amount of times to check for the cluster before considering it unstable and exiting",
	)
	imageFlag := "image"
	command.Flags().StringVar(
		&image,
		imageFlag,
		common.DefaultImage(),
		"An arbitrary container image to use.",
	)
	command.Flags().MarkHidden(imageFlag)

	command.Flags().StringSliceVar(
		&regPorts,
		flagRegPorts,
		[]string{strconv.Itoa(config.DefaultSchemaRegPort)},
		"Schema registry ports to listen on",
	)
	command.Flags().StringSliceVar(
		&proxyPorts,
		flagProxyPorts,
		[]string{strconv.Itoa(config.DefaultProxyPort)},
		"Pandaproxy ports to listen on",
	)
	command.Flags().StringSliceVar(
		&kafkaPorts,
		flagKafkaPorts,
		[]string{strconv.Itoa(config.DefaultKafkaPort)},
		"Kafka protocol ports to listen on",
	)
	command.Flags().StringSliceVar(
		&adminPorts,
		flagAdminPorts,
		[]string{strconv.Itoa(config.DefaultAdminPort)},
		"Redpanda Admin API ports to listen on",
	)
	command.Flags().StringSliceVar(
		&rpcPorts,
		flagRPCPorts,
		[]string{strconv.Itoa(config.DefaultRPCPort)},
		"Inter-broker RPC ports to listen on",
	)

	return command
}

func (p *clusterPorts) assignAny(takenPorts map[uint]struct{}) error {
	var need int
	for _, src := range [][]uint{p.reg, p.proxy, p.kafka, p.admin, p.rpc} {
		for _, port := range src {
			if port == 0 {
				need++
			}
		}
	}
	random, err := vnet.GetFreePortPool(need, takenPorts)
	if err != nil {
		return fmt.Errorf("unable to allocate random ports: %v", err)
	}
	for _, src := range [][]uint{p.reg, p.proxy, p.kafka, p.admin, p.rpc} {
		for i, port := range src {
			if port == 0 {
				src[i] = random[0]
				random = random[1:]
			}
		}
	}
	return nil
}

func assignPorts(
	name string,
	strPorts []string,
	takenPorts map[uint]struct{},
	nodes uint,
	require bool,
) (ports, error) {
	const maxPort = 65535

	if len(strPorts) == 0 {
		return nil, nil
	}
	ports := make(ports, 0, len(strPorts))
	var isAny, isNone, isPort bool
	for _, strPort := range strPorts {
		switch strPort {
		case "none", "skip":
			isNone = true
		case "any":
			isAny = true
		default:
			isPort = true
			port, err := strconv.ParseUint(strPort, 10, 64)
			if err != nil {
				return nil, fmt.Errorf(`--%s: invalid port %s must be "none", "any", or a number`, name, strPort)
			}
			if _, isTaken := takenPorts[uint(port)]; isTaken {
				return nil, fmt.Errorf("--%s: port %d is already defined", name, port)
			}
			if port <= 1024 || port > maxPort {
				return nil, fmt.Errorf("--%s: port must be between 1025 and %d", name, maxPort)
			}
			takenPorts[uint(port)] = struct{}{}
			ports = append(ports, uint(port))
		}
	}
	if isAny && isNone {
		return nil, fmt.Errorf(`--%s: cannot use both "any" and "skip"`, name)
	}
	if isPort && isAny {
		return nil, fmt.Errorf(`--%s: cannot use both specific ports and "any"`, name)
	}
	if isPort && isNone {
		return nil, fmt.Errorf(`--%s: cannot use both specific ports and "none"`, name)
	}
	if isPort && len(strPorts) > int(nodes) {
		return nil, fmt.Errorf("--%s: number of ports defined (%d) exceeds the number of nodes requested (%d)", name, len(strPorts), int(nodes))
	}

	switch {
	case isAny:
		return ports[:nodes], nil
	case isPort && len(ports) > 0:
		sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })
		for port := ports[0]; port < 65535 && len(ports) != int(nodes); port += 100 {
			if _, isTaken := takenPorts[port]; !isTaken {
				ports = append(ports, port)
				takenPorts[port] = struct{}{}
			}
		}
		if len(ports) != int(nodes) {
			return nil, fmt.Errorf("--%s: unable to pick an unoccupied port from %d to %d with a step of 100", name, ports[0], maxPort)
		}
	}

	if require && len(ports) == 0 {
		return nil, fmt.Errorf(`--%s cannot be empty; this requires at either at least one port defined, or "any"`, name)
	}
	return ports, nil
}

func startCluster(
	c common.Client,
	n uint,
	p clusterPorts,
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
			log.Info("\nTo change the number of nodes, first purge the existing cluster:\n\nrpk container purge\n")
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

	// We copy our Kafka and Admin ports to make building our output below
	// easier.
	allKafka := append(ports(nil), p.kafka...)
	allAdmin := append(ports(nil), p.admin...)

	seedKafkaPort := p.kafka.pop()
	seedState, err := common.CreateNode(
		c,
		0, // seed ID is 0
		p.reg.pop(),
		p.proxy.pop(),
		seedKafkaPort,
		p.admin.pop(),
		p.rpc.pop(),
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

	var (
		nodes  = []node{{0, nodeAddr(seedKafkaPort)}}
		mu     sync.Mutex
		grp, _ = errgroup.WithContext(context.Background())
	)

	for nodeID := uint(1); nodeID < n; nodeID++ {
		id := nodeID
		grp.Go(func() error {
			args := []string{
				"--seeds",
				net.JoinHostPort(
					seedState.ContainerIP,
					strconv.Itoa(config.DefaultRPCPort),
				),
			}
			state, err := common.CreateNode(
				c,
				id,
				p.reg.pop(),
				p.proxy.pop(),
				p.kafka.pop(),
				p.admin.pop(),
				p.rpc.pop(),
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
		return fmt.Errorf("error restarting the cluster: %v", err)
	}
	err = waitForCluster(check(nodes), retries)
	if err != nil {
		return err
	}
	renderClusterInfo(nodes)

	var brokers, admin []string
	for _, m := range []struct {
		src []uint
		dst *[]string
	}{
		{allKafka, &brokers},
		{allAdmin, &admin},
	} {
		for _, port := range m.src {
			*m.dst = append(*m.dst, nodeAddr(port))
		}
	}

	output := fmt.Sprintf(`Cluster started! You may use rpk to interact with it. E.g:

  rpk cluster info --brokers %[1]s

You may also set an environment variables:

  export REDPANDA_BROKERS="%[1]s"
`, strings.Join(brokers, ","))
	if len(admin) > 0 {
		output += fmt.Sprintf(`  export REDPANDA_API_ADMIN_ADDRS="%s"
`, strings.Join(admin, ","))
	}
	output += "  rpk cluster info\n"

	fmt.Print(output)

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
	var mu sync.Mutex
	var nodes []node
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

func nodeAddr(port uint) string { return fmt.Sprintf("127.0.0.1:%d", port) }
