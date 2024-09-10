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
	"bytes"
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"

	"github.com/docker/docker/api/types/container"

	"github.com/avast/retry-go"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	vnet "github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

type node struct {
	id   uint
	addr string
}

type clusterPorts struct {
	adminPorts  []uint
	kafkaPorts  []uint
	proxyPorts  []uint
	rpcPorts    []uint
	schemaPorts []uint
	consolePort uint
}

func newStartCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	const (
		flagSRPorts     = "schema-registry-ports"
		flagProxyPorts  = "proxy-ports"
		flagKafkaPorts  = "kafka-ports"
		flagAdminPorts  = "admin-ports"
		flagRPCPorts    = "rpc-ports"
		flagConsolePort = "console-port"
		flagAnyPort     = "any-port"
	)
	var (
		nodes        uint
		retries      uint
		image        string
		consoleImage string
		pull         bool
		noProfile    bool

		aPorts      []string
		kPorts      []string
		pPorts      []string
		rPorts      []string
		srPorts     []string
		consolePort string
		anyPort     bool

		subnet  string
		gateway string
	)
	command := &cobra.Command{
		Use:   "start",
		Short: "Start a local container cluster",
		Long: `Start a local container cluster.

This command uses Docker to initiate a local Redpanda container cluster,
including Redpanda Console. Use the '--nodes'/'-n' flag to specify the number of
brokers.

The initial broker starts on default ports, with subsequent brokers' ports
offset by 1000. You can use the listeners flag to specify ports:

  * --kafka-ports
  * --admin-ports
  * --rpc-ports
  * --schema-registry-ports
  * --proxy-ports
  * --console-port

Each flag accepts a comma-separated list of ports for your listeners. Use the
'--any-port' flag to let rpk select random available ports for every listener on
the host machine.

By default, this command uses the redpandadata/redpanda:latest and 
redpandadata/console:latest container images. You can specify a container image 
by using the '--image' flag.

In case of IP address pool conflict, you may specify a custom subnet and gateway
using the '--subnet' and '--gateway' flags respectively.
`,
		Example: `
Start a three-broker cluster:
  rpk container start -n 3

Start a single-broker cluster, selecting random ports for every listener:
  rpk container start --any-port

Start a three-broker cluster, selecting the seed Kafka and Redpanda Console 
ports only:
  rpk container start --kafka-ports 9092 --console-port 8080

Start a three-broker cluster, selecting the Admin API port for each broker:
  rpk container start --admin-ports 9644,9645,9646
`,
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			// Allow unknown flags so that arbitrary flags can be passed
			// through to the containers without the need to pass '--'
			// (POSIX standard)
			UnknownFlags: true,
		},
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			if nodes < 1 {
				out.Die("--nodes should be 1 or greater")
			}
			c, err := common.NewDockerClient(cmd.Context())
			out.MaybeDie(err, "unable to create docker client: %v", err)
			defer c.Close()

			if anyPort {
				aPorts, kPorts, pPorts, rPorts, srPorts, consolePort = []string{"any"}, []string{"any"}, []string{"any"}, []string{"any"}, []string{"any"}, "any"
			}
			cPorts, err := parseContainerPortFlags(int(nodes), aPorts, kPorts, pPorts, rPorts, srPorts, consolePort)
			out.MaybeDie(err, "unable to parse container ports: %v", err)

			configKvs := collectFlags(os.Args, "--set")
			isRestarted, err := startCluster(c, nodes, checkBrokers, retries, image, consoleImage, pull, cPorts, configKvs, subnet, gateway)
			if err != nil {
				if errors.As(err, &portInUseError{}) {
					out.Die("unable to start cluster: %v\nYou may select different ports to start the cluster using our listener flags. Check '--help' text for more information", err)
				}
				out.Die("unable to start cluster: %v", common.WrapIfConnErr(err))
			}

			if noProfile || isRestarted {
				return
			}

			dockerNodes, err := renderClusterInfo(c)
			out.MaybeDie(err, "unable to render cluster info: %v; you may run 'rpk container status' to retrieve the cluster info", err)

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			err = common.CreateProfile(fs, c, y)
			if err == nil {
				fmt.Printf("\nCreated %q profile.\n", common.ContainerProfileName)
				renderClusterInteract(dockerNodes, true)
				return
			}
			if errors.Is(err, common.ErrContainerProfileExists) {
				fmt.Printf(`Unable to create a profile for the rpk container: %v.

You can retry profile creation by running:
    rpk profile delete %s; rpk profile create --from-rpk-container
`, err, common.ContainerProfileName)
				return
			} else {
				out.Die("unable to create a profile for the rpk container: %v", err)
			}
		},
	}

	command.Flags().UintVarP(&nodes, "nodes", "n", 1, "The number of brokers (nodes) to start")
	command.Flags().UintVar(&retries, "retries", 10, "The amount of times to check for the cluster before considering it unstable and exiting")
	command.Flags().StringVar(&image, "image", common.DefaultRedpandaImage(), "An arbitrary Redpanda container image to use")
	command.Flags().StringVar(&consoleImage, "console-image", common.DefaultConsoleImage(), "An arbitrary Redpanda Console container image to use")
	command.Flags().BoolVar(&pull, "pull", false, "Force pull the container image used")
	command.Flags().BoolVar(&noProfile, "no-profile", false, "If true, rpk will not create an rpk profile after creating a cluster")
	command.Flags().String("set", "", "Redpanda configuration property to set upon start. Follows 'rpk redpanda config set' format")
	command.Flags().StringSliceVar(&kPorts, flagKafkaPorts, nil, "Kafka protocol ports to listen on; check help text for more information")
	command.Flags().StringSliceVar(&aPorts, flagAdminPorts, nil, "Redpanda Admin API ports to listen on; check help text for more information")
	command.Flags().StringSliceVar(&srPorts, flagSRPorts, nil, "Schema Registry ports to listen on; check help text for more information")
	command.Flags().StringSliceVar(&pPorts, flagProxyPorts, nil, "HTTP Proxy ports to listen on; check help text for more information")
	command.Flags().StringSliceVar(&rPorts, flagRPCPorts, nil, "RPC ports to listen on; check help text for more information")
	command.Flags().StringVar(&consolePort, flagConsolePort, "8080", "Redpanda console ports to listen on; check help text for more information")
	// opt-in for 'any' in all listeners
	command.Flags().BoolVar(&anyPort, flagAnyPort, false, "Opt in for any (random) ports in all listeners")

	command.Flags().StringVar(&subnet, "subnet", "172.24.1.0/24", "Subnet to create the cluster network on")
	command.Flags().StringVar(&gateway, "gateway", "172.24.1.1", "Gateway IP address for the subnet. Must be in the subnet address range")

	command.MarkFlagsMutuallyExclusive(flagAnyPort, flagKafkaPorts)
	command.MarkFlagsMutuallyExclusive(flagAnyPort, flagAdminPorts)
	command.MarkFlagsMutuallyExclusive(flagAnyPort, flagSRPorts)
	command.MarkFlagsMutuallyExclusive(flagAnyPort, flagProxyPorts)
	command.MarkFlagsMutuallyExclusive(flagAnyPort, flagRPCPorts)

	return command
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

func startCluster(
	c common.Client,
	n uint,
	check func([]node) func() error,
	retries uint,
	image, consoleImage string,
	pull bool,
	clusterPorts clusterPorts,
	extraArgs []string,
	subnet, gateway string,
) (isRestarted bool, rerr error) {
	// Check if cluster exists and start it again.
	restarted, err := restartCluster(c, check, retries)
	if err != nil {
		return false, err
	}
	// If a cluster was restarted, there's nothing else to do.
	if len(restarted) != 0 {
		fmt.Print("Found an existing cluster:\n\n")
		renderClusterInfo(c)
		if len(restarted) != int(n) {
			fmt.Print("\nTo change the number of nodes, first purge the existing cluster with\n'rpk container purge'.\n\n")
		}
		return true, nil
	}

	if pull {
		fmt.Println("Force pulling images...")
		err = common.PullImage(c, image)
		if err != nil {
			return false, fmt.Errorf("unable to pull Redpanda image: %v", err)
		}
		err = common.PullImage(c, consoleImage)
		if err != nil {
			return false, fmt.Errorf("unable to pull Redpanda Console image: %v", err)
		}
	} else {
		fmt.Println("Checking for a local image...")
		err := checkPresentAndPull(c, image)
		if err != nil {
			return false, fmt.Errorf("unable to check Redpanda image: %v", err)
		}
		err = checkPresentAndPull(c, consoleImage)
		if err != nil {
			return false, fmt.Errorf("unable to check Redpanda Console image: %v", err)
		}
	}

	// Create the docker network if it doesn't exist already
	netID, err := common.CreateNetwork(c, subnet, gateway)
	if err != nil {
		return false, err
	}

	err = verifyPortsInUse(clusterPorts)
	if err != nil {
		return false, err
	}

	// Start a seed node.
	var (
		seedID            uint
		seedKafkaPort     = clusterPorts.kafkaPorts[0]
		seedProxyPort     = clusterPorts.proxyPorts[0]
		seedSchemaRegPort = clusterPorts.schemaPorts[0]
		seedRPCPort       = clusterPorts.rpcPorts[0]
		seedAdminPort     = clusterPorts.adminPorts[0]
	)

	seedState, err := common.CreateNode(
		c,
		seedID,
		seedKafkaPort,
		seedProxyPort,
		seedSchemaRegPort,
		seedRPCPort,
		seedAdminPort,
		netID,
		image,
		extraArgs...,
	)
	if err != nil {
		return false, err
	}

	fmt.Println("Starting cluster...")
	err = startNode(c, seedState.ContainerID)
	if err != nil {
		return false, err
	}

	seedNode := node{
		seedID,
		nodeAddr(seedKafkaPort),
	}
	kafkaAddr := []string{fmt.Sprintf("%v:%d", seedState.ContainerIP, config.DefaultKafkaPort)}
	srAddr := []string{fmt.Sprintf("http://rp-node-%d:%d", seedID, config.DefaultSchemaRegPort)}
	adminAddr := []string{fmt.Sprintf("http://rp-node-%d:%d", seedID, config.DefaultAdminPort)}

	nodes := []node{seedNode}

	mu := sync.Mutex{}

	grp, _ := errgroup.WithContext(context.Background())

	for nodeID := uint(1); nodeID < n; nodeID++ {
		id := nodeID
		grp.Go(func() error {
			var (
				kafkaPort     = clusterPorts.kafkaPorts[id]
				proxyPort     = clusterPorts.proxyPorts[id]
				schemaRegPort = clusterPorts.schemaPorts[id]
				rpcPort       = clusterPorts.rpcPorts[id]
				adminPort     = clusterPorts.adminPorts[id]
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
				adminPort,
				netID,
				image,
				append(args, extraArgs...)...,
			)
			if err != nil {
				return err
			}
			err = startNode(c, state.ContainerID)
			if err != nil {
				return err
			}
			mu.Lock()
			nodes = append(nodes, node{
				id:   id,
				addr: nodeAddr(state.HostKafkaPort),
			})
			kafkaAddr = append(kafkaAddr, fmt.Sprintf("%v:%d", state.ContainerIP, config.DefaultKafkaPort))
			srAddr = append(srAddr, fmt.Sprintf("http://rp-node-%d:%d", id, config.DefaultSchemaRegPort))
			adminAddr = append(adminAddr, fmt.Sprintf("http://rp-node-%d:%d", id, config.DefaultAdminPort))
			mu.Unlock()
			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return false, fmt.Errorf("error restarting the cluster: %v", err)
	}
	fmt.Println("Waiting for the cluster to be ready...")
	err = waitForCluster(check(nodes), retries)
	if err != nil {
		state, sErr := common.GetState(c, nodes[0].id, false)
		if sErr != nil {
			return false, fmt.Errorf("%v\nunable to get Docker container logs: %v", err, sErr)
		}
		errStr, cErr := getContainerErr(state, c)
		if cErr != nil {
			return false, fmt.Errorf("%v\nunable to get Docker container logs: %v", err, cErr)
		}
		return false, fmt.Errorf("%v\n\nErrors reported from the Docker container:\n\n%v", err, errStr)
	}
	fmt.Println("Cluster ready!")

	fmt.Println("Starting Redpanda Console...")
	consoleID := uint(len(nodes))
	consoleState, err := common.CreateConsoleNode(c, consoleID, netID, consoleImage, clusterPorts.consolePort, kafkaAddr, srAddr, adminAddr)
	if err != nil {
		return false, err
	}
	if err := startNode(c, consoleState.ContainerID); err != nil {
		return false, err
	}
	consoleNode := node{consoleID, nodeAddr(consoleState.HostConsolePort)}
	fmt.Println("Waiting for Redpanda Console to be ready...")
	err = waitForCluster(checkConsole(consoleNode), retries)
	if err != nil {
		state, sErr := common.GetState(c, consoleNode.id, true)
		if sErr != nil {
			return false, fmt.Errorf("%v\nunable to get Docker container logs: %v", err, sErr)
		}
		errStr, cErr := getContainerErr(state, c)
		if cErr != nil {
			return false, fmt.Errorf("%v\nunable to get Docker container logs: %v", err, cErr)
		}
		return false, fmt.Errorf("%v\n\nErrors reported from the Docker container:\n\n%v", err, errStr)
	}

	fmt.Printf("Console ready!\n\n")

	return false, nil
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
	var (
		rpNodes      []node
		consoleNode  node
		consoleState *common.NodeState
	)

	for _, s := range states {
		state := s
		grp.Go(func() error {
			if !state.Running {
				ctx, _ := common.DefaultCtx()
				// Console node needs to start after the Redpanda nodes start.
				if !state.Console {
					err = c.ContainerStart(ctx, state.ContainerID, container.StartOptions{})
					if err != nil {
						return err
					}
				}
				state, err = common.GetState(c, state.ID, state.Console)
				if err != nil {
					return err
				}
			}
			mu.Lock()
			if state.Console {
				consoleState = state
				consoleNode = node{state.ID, nodeAddr(state.HostConsolePort)}
			} else {
				rpNodes = append(rpNodes, node{
					id:   state.ID,
					addr: nodeAddr(state.HostKafkaPort),
				})
			}
			mu.Unlock()
			return nil
		})
	}
	err = grp.Wait()
	if err != nil {
		return nil, fmt.Errorf("error restarting the cluster: %v", err)
	}
	fmt.Printf("Waiting for the cluster to be ready...\n\n")
	err = waitForCluster(check(rpNodes), retries)
	if err != nil {
		errStr, cErr := getContainerErr(states[0], c)
		if cErr != nil {
			return nil, fmt.Errorf("%v\nunable to get Docker container logs: %v", err, cErr)
		}
		return nil, fmt.Errorf("%v\n\nErrors reported from the Docker container:\n%v", err, errStr)
	}

	if !consoleState.Running {
		ctx, _ := common.DefaultCtx()
		err = c.ContainerStart(ctx, consoleState.ContainerID, container.StartOptions{})
		if err != nil {
			return nil, fmt.Errorf("unable to start the Redpanda Console container: %v", err)
		}
		state, err := common.GetState(c, consoleState.ID, consoleState.Console)
		if err != nil {
			return nil, fmt.Errorf("unable to inspect Redpanda Console container after start: %v", err)
		}
		consoleNode = node{state.ID, nodeAddr(state.HostConsolePort)}
	}
	err = waitForCluster(checkConsole(consoleNode), retries)
	if err != nil {
		errStr, cErr := getContainerErr(consoleState, c)
		if cErr != nil {
			return nil, fmt.Errorf("%v\nunable to get Docker container logs: %v", err, cErr)
		}
		return nil, fmt.Errorf("%v\n\nErrors reported from the Docker container:\n%v", err, errStr)
	}
	return rpNodes, nil
}

func startNode(c common.Client, containerID string) error {
	ctx, _ := common.DefaultCtx()
	err := c.ContainerStart(ctx, containerID, container.StartOptions{})
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
		if len(brokers) < len(nodes) {
			return fmt.Errorf(
				"expected %d nodes, got %d",
				len(nodes),
				len(brokers),
			)
		}
		return nil
	}
}

func checkConsole(node node) func() error {
	return func() error {
		cl := httpapi.NewClient(
			httpapi.Host("http://"+node.addr),
			httpapi.Retries(1),
		)
		var res map[string]bool
		err := cl.Get(context.Background(), "/admin/startup", nil, &res)
		if err != nil {
			return fmt.Errorf("error checking console status: %v", err)
		}
		if res["isHttpOk"] {
			if res["isKafkaOk"] {
				return nil
			}
			return errors.New("console is not healthy, Kafka API is not reachable from console")
		}
		return errors.New("console is not healthy")
	}
}

func waitForCluster(check func() error, retries uint) error {
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

	tw := out.NewTable("Node-ID", "Status", "Kafka-Address", "Admin-Address", "Proxy-Address", "Schema-Registry-Address")
	var consoleNode *common.NodeState
	defer func() {
		tw.Flush()
		if consoleNode != nil && consoleNode.Running {
			fmt.Printf("\nRedpanda Console started in: %s", fmt.Sprintf("http://localhost:%d\n", consoleNode.HostConsolePort))
		}
	}()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	for _, node := range nodes {
		if node.Console {
			consoleNode = node
			continue
		}
		kafka := nodeAddr(node.HostKafkaPort)
		admin := nodeAddr(node.HostAdminPort)
		proxy := nodeAddr(node.HostProxyPort)
		schema := nodeAddr(node.HostSchemaPort)
		if node.HostKafkaPort == 0 {
			kafka = "-"
		}
		if node.HostAdminPort == 0 {
			admin = "-"
		}
		if node.HostProxyPort == 0 {
			proxy = "-"
		}
		if node.HostSchemaPort == 0 {
			schema = "-"
		}
		tw.PrintStrings(
			fmt.Sprint(node.ID),
			node.Status,
			kafka,
			admin,
			proxy,
			schema,
		)
	}
	return nodes, nil
}

func renderClusterInteract(nodes []*common.NodeState, withProfile bool) {
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
	if withProfile {
		fmt.Printf(`
You can use rpk to interact with this cluster. E.g:

    rpk cluster info
    rpk cluster health

`)
	} else {
		msg := `
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
		fmt.Printf(msg, b, a, b, a)
	}
}

func nodeAddr(port uint) string {
	return fmt.Sprintf(
		"127.0.0.1:%d",
		port,
	)
}

// getContainerErr attempts to fetch the latest stderr output from the first
// Redpanda node. It may reveal reasons for failing to start.
func getContainerErr(state *common.NodeState, c common.Client) (string, error) {
	ctx, _ := common.DefaultCtx()

	json, err := c.ContainerInspect(ctx, state.ContainerID)
	if err != nil {
		return "", fmt.Errorf("could not inspect container: %v", err)
	}

	reader, err := c.ContainerLogs(
		ctx,
		state.ContainerID,
		container.LogsOptions{
			ShowStdout: false,
			ShowStderr: true,
			Since:      json.State.StartedAt,
		},
	)
	if err != nil {
		return "", fmt.Errorf("could not get container logs: %v", err)
	}

	// Docker logs over the wire are multiplexed using stdcopy package. To
	// demux this stream we need to use stdcopy.StdCopy. See:
	// https://github.com/moby/moby/issues/32794#issuecomment-297151440
	bErr := new(bytes.Buffer)
	_, err = stdcopy.StdCopy(nil, bErr, reader)
	if err != nil {
		return "", fmt.Errorf("unable to read docker logs: %v", err)
	}

	return bErr.String(), nil
}

// parsePorts parses a single port array.
//   - If the port array is empty, we return a default port array.
//   - If the port is 'any' we get a random port using vnet.GetFreePortPool.
//   - If the provided port array < number of nodes, we fill the port array with succeeding ports.
func parsePorts(ports []string, nNodes, defPort int) ([]uint, error) {
	var ret []uint
	// If no port is defined, we use the default as seed node port.
	if len(ports) == 0 {
		ret = append(ret, uint(defPort))
	}
	for _, p := range ports {
		// If port == 'any', we assign a random port.
		if p == "any" {
			if len(ports) > 1 {
				return nil, errors.New("cannot specify 'any' with additional ports")
			}
			portPool, err := vnet.GetFreePortPool(nNodes)
			if err != nil {
				return nil, fmt.Errorf("unable to assign random ports: %v", err)
			}
			return portPool, nil
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %v: %v", p, err)
		}
		if n < 0 {
			return nil, fmt.Errorf("cannot parse %v: port cannot be a negative number", p)
		}
		ret = append(ret, uint(n))
	}
	// If the user didn't specify enough ports, we fill the rest based on the last port passed.
	if len(ret) < nNodes {
		sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
		for nNodes-len(ret) > 0 {
			ret = append(ret, ret[len(ret)-1]+1000)
		}
	}
	return ret, nil
}

func parseContainerPortFlags(nNodes int, adminPorts, kafkaPorts, proxyPorts, rpcPorts, schemaPorts []string, consolePort string) (clusterPorts, error) {
	aPorts, err := parsePorts(adminPorts, nNodes, config.DefaultAdminPort)
	if err != nil {
		return clusterPorts{}, fmt.Errorf("unable to parse admin ports: %v", err)
	}
	kPorts, err := parsePorts(kafkaPorts, nNodes, config.DefaultKafkaPort)
	if err != nil {
		return clusterPorts{}, fmt.Errorf("unable to parse kafka ports: %v", err)
	}
	pPorts, err := parsePorts(proxyPorts, nNodes, config.DefaultProxyPort)
	if err != nil {
		return clusterPorts{}, fmt.Errorf("unable to parse proxy ports: %v", err)
	}
	rPorts, err := parsePorts(rpcPorts, nNodes, config.DefaultRPCPort)
	if err != nil {
		return clusterPorts{}, fmt.Errorf("unable to parse rpc ports: %v", err)
	}
	srPorts, err := parsePorts(schemaPorts, nNodes, config.DefaultSchemaRegPort)
	if err != nil {
		return clusterPorts{}, fmt.Errorf("unable to parse schema registry ports: %v", err)
	}
	consolePorts, err := parsePorts([]string{consolePort}, 1, config.DefaultConsolePort)
	if err != nil {
		return clusterPorts{}, fmt.Errorf("unable to parse Redpanda Console port: %v", err)
	}
	return clusterPorts{
		adminPorts:  aPorts,
		kafkaPorts:  kPorts,
		proxyPorts:  pPorts,
		rpcPorts:    rPorts,
		schemaPorts: srPorts,
		consolePort: consolePorts[0],
	}, nil
}

func verifyPortsInUse(cPorts clusterPorts) error {
	check := func(ports []uint, listener string) error {
		for _, p := range ports {
			server, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
			// if it fails then the port might be in use
			if err != nil {
				return portInUseError{p, listener}
			}
			server.Close()
		}
		return nil
	}
	if err := check(cPorts.kafkaPorts, "kafka"); err != nil {
		return err
	}
	if err := check(cPorts.adminPorts, "admin"); err != nil {
		return err
	}
	if err := check(cPorts.rpcPorts, "rpc"); err != nil {
		return err
	}
	if err := check(cPorts.schemaPorts, "schema registry"); err != nil {
		return err
	}
	if err := check(cPorts.proxyPorts, "pandaproxy"); err != nil {
		return err
	}
	return check([]uint{cPorts.consolePort}, "console")
}

type portInUseError struct {
	port     uint
	listener string
}

func (p portInUseError) Error() string {
	return fmt.Sprintf("%v port %v already in use", p.listener, p.port)
}

func checkPresentAndPull(c common.Client, image string) error {
	rpPresent, checkErr := common.CheckIfImgPresent(c, image)
	if checkErr != nil {
		fmt.Printf("Error trying to list local images: %v\n", checkErr)
	}
	if !rpPresent {
		// If the image isn't present locally, try to pull it.
		fmt.Printf("Version %q not found locally\n", image)
		err := common.PullImage(c, image)
		if err != nil {
			return fmt.Errorf("could not pull image: %v", err)
		}
	}
	return nil
}
