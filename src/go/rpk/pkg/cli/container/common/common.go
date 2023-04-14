// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"go.uber.org/zap"
)

var (
	tag               = "latest"
	redpandaImageBase = "vectorized/redpanda:" + tag
)

const (
	redpandaNetwork   = "redpanda"
	externalKafkaPort = 9093

	defaultDockerClientTimeout = 60 * time.Second
)

type NodeState struct {
	Status        string
	Running       bool
	ConfigFile    string
	HostRPCPort   uint
	HostKafkaPort uint
	HostAdminPort uint
	HostProxyPort uint
	ID            uint
	ContainerIP   string
	ContainerID   string
}

func ListenAddresses(ip string, internalPort, externalPort uint) string {
	return fmt.Sprintf(
		"internal://%s,external://%s",
		net.JoinHostPort("0.0.0.0", fmt.Sprint(internalPort)),
		net.JoinHostPort(ip, fmt.Sprint(externalPort)),
	)
}

func AdvertiseAddresses(ip string, internalPort, externalPort uint) string {
	return fmt.Sprintf(
		"internal://%s,external://%s",
		net.JoinHostPort(ip, fmt.Sprint(internalPort)),
		net.JoinHostPort("127.0.0.1", fmt.Sprint(externalPort)),
	)
}

// Returns the container name for the given node ID.
func Name(nodeID uint) string {
	return fmt.Sprintf("rp-node-%d", nodeID)
}

func DefaultImage() string {
	return redpandaImageBase
}

func DefaultCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultDockerClientTimeout)
}

func GetExistingNodes(c Client) ([]*NodeState, error) {
	regExp := `^/rp-node-[\d]+`
	filters := filters.NewArgs()
	filters.Add("name", regExp)
	ctx, _ := DefaultCtx()
	containers, err := c.ContainerList(
		ctx,
		types.ContainerListOptions{
			All:     true,
			Filters: filters,
		},
	)
	if err != nil {
		return nil, err
	}
	if len(containers) == 0 {
		return []*NodeState{}, nil
	}

	nodes := make([]*NodeState, len(containers))
	for i, cont := range containers {
		nodeIDStr := cont.Labels["node-id"]
		nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"Couldn't parse node ID: '%s'",
				nodeIDStr,
			)
		}
		nodes[i], err = GetState(c, uint(nodeID))
		if err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func GetState(c Client, nodeID uint) (*NodeState, error) {
	ctx, _ := DefaultCtx()
	containerJSON, err := c.ContainerInspect(ctx, Name(nodeID))
	if err != nil {
		return nil, err
	}
	if containerJSON.NetworkSettings == nil || containerJSON.ContainerJSONBase == nil {
		return nil, fmt.Errorf("unable to inspect the container %v, please make sure you have Docker installed and running", Name(nodeID))
	}
	var ipAddress string
	network, exists := containerJSON.NetworkSettings.Networks[redpandaNetwork]
	if exists {
		ipAddress = network.IPAMConfig.IPv4Address
	}

	hostRPCPort, err := getHostPort(
		config.DevDefault().Redpanda.RPCServer.Port,
		containerJSON,
	)
	if err != nil {
		return nil, err
	}
	hostKafkaPort, err := getHostPort(
		externalKafkaPort,
		containerJSON,
	)
	if err != nil {
		return nil, err
	}
	hostAdminPort, err := getHostPort(
		config.DefaultAdminPort,
		containerJSON,
	)
	if err != nil {
		return nil, err
	}
	hostProxyPort, err := getHostPort(
		config.DefaultProxyPort,
		containerJSON,
	)
	if err != nil {
		return nil, err
	}
	return &NodeState{
		Running:       containerJSON.State.Running,
		Status:        containerJSON.State.Status,
		ContainerID:   containerJSON.ID,
		ContainerIP:   ipAddress,
		HostKafkaPort: hostKafkaPort,
		HostRPCPort:   hostRPCPort,
		HostAdminPort: hostAdminPort,
		HostProxyPort: hostProxyPort,
		ID:            nodeID,
	}, nil
}

// Creates a network for the cluster's containers and returns its ID. If it
// exists already, it returns the existing network's ID.
func CreateNetwork(c Client) (string, error) {
	ctx, _ := DefaultCtx()

	args := filters.NewArgs()
	args.Add("name", redpandaNetwork)
	networks, err := c.NetworkList(
		ctx,
		types.NetworkListOptions{Filters: args},
	)
	if err != nil {
		return "", err
	}

	for _, net := range networks {
		if net.Name == redpandaNetwork {
			return net.ID, nil
		}
	}

	fmt.Printf("Creating network %s\n", redpandaNetwork)
	resp, err := c.NetworkCreate(
		ctx, redpandaNetwork, types.NetworkCreate{
			Driver: "bridge",
			IPAM: &network.IPAM{
				Driver: "default",
				Config: []network.IPAMConfig{
					{
						Subnet:  "172.24.1.0/24",
						Gateway: "172.24.1.1",
					},
				},
			},
		},
	)
	if err != nil {
		return "", err
	}
	if resp.Warning != "" {
		fmt.Printf("Network creation warning: %v\n", resp.Warning)
	}
	return resp.ID, nil
}

// Delete the Redpanda network if it exists.
func RemoveNetwork(c Client) error {
	ctx, _ := DefaultCtx()
	err := c.NetworkRemove(ctx, redpandaNetwork)
	if c.IsErrNotFound(err) {
		return nil
	}
	return err
}

func CreateNode(
	c Client,
	nodeID, kafkaPort, proxyPort, schemaRegPort, rpcPort, metricsPort uint,
	netID, image string,
	args ...string,
) (*NodeState, error) {
	rPort, err := nat.NewPort(
		"tcp",
		strconv.Itoa(config.DevDefault().Redpanda.RPCServer.Port),
	)
	if err != nil {
		return nil, err
	}
	kPort, err := nat.NewPort( //nolint:revive // var-naming diff here is intended kPort = kafkaPort.
		"tcp",
		strconv.Itoa(int(externalKafkaPort)),
	)
	if err != nil {
		return nil, err
	}
	pPort, err := nat.NewPort(
		"tcp",
		strconv.Itoa(config.DefaultProxyPort),
	)
	if err != nil {
		return nil, err
	}
	sPort, err := nat.NewPort(
		"tcp",
		strconv.Itoa(config.DefaultSchemaRegPort),
	)
	if err != nil {
		return nil, err
	}
	metPort, err := nat.NewPort(
		"tcp",
		strconv.Itoa(config.DefaultAdminPort),
	)
	if err != nil {
		return nil, err
	}
	ip, err := nodeIP(c, netID, nodeID)
	if err != nil {
		return nil, err
	}
	hostname := Name(nodeID)
	cmd := []string{
		"redpanda",
		"start",
		"--node-id",
		fmt.Sprintf("%d", nodeID),
		"--kafka-addr",
		ListenAddresses(ip, config.DefaultKafkaPort, externalKafkaPort),
		"--pandaproxy-addr",
		ListenAddresses(ip, config.DefaultProxyPort, proxyPort),
		"--schema-registry-addr",
		net.JoinHostPort(ip, strconv.Itoa(config.DefaultSchemaRegPort)),
		"--rpc-addr",
		net.JoinHostPort(ip, strconv.Itoa(config.DevDefault().Redpanda.RPCServer.Port)),
		"--advertise-kafka-addr",
		AdvertiseAddresses(ip, config.DefaultKafkaPort, kafkaPort),
		"--advertise-pandaproxy-addr",
		AdvertiseAddresses(ip, config.DefaultProxyPort, proxyPort),
		"--advertise-rpc-addr",
		net.JoinHostPort(ip, strconv.Itoa(config.DevDefault().Redpanda.RPCServer.Port)),
		"--mode dev-container",
	}
	containerConfig := container.Config{
		Image:    image,
		Hostname: hostname,
		Cmd:      append(cmd, args...),
		ExposedPorts: nat.PortSet{
			rPort: {},
			pPort: {},
			sPort: {},
			kPort: {},
		},
		Labels: map[string]string{
			"cluster-id": "redpanda",
			"node-id":    fmt.Sprint(nodeID),
		},
	}
	hostConfig := container.HostConfig{
		PortBindings: nat.PortMap{
			rPort: []nat.PortBinding{{
				HostPort: fmt.Sprint(rpcPort),
			}},
			kPort: []nat.PortBinding{{
				HostPort: fmt.Sprint(kafkaPort),
			}},
			pPort: []nat.PortBinding{{
				HostPort: fmt.Sprint(proxyPort),
			}},
			sPort: []nat.PortBinding{{
				HostPort: fmt.Sprint(schemaRegPort),
			}},
			metPort: []nat.PortBinding{{
				HostPort: fmt.Sprint(metricsPort),
			}},
		},
	}
	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			redpandaNetwork: {
				IPAMConfig: &network.EndpointIPAMConfig{
					IPv4Address: ip,
				},
				Aliases: []string{hostname},
			},
		},
	}

	ctx, _ := DefaultCtx()
	container, err := c.ContainerCreate(
		ctx,
		&containerConfig,
		&hostConfig,
		&networkConfig,
		nil,
		hostname,
	)
	if err != nil {
		return nil, err
	}
	return &NodeState{
		HostKafkaPort: kafkaPort,
		ID:            nodeID,
		ContainerID:   container.ID,
		ContainerIP:   ip,
	}, nil
}

func PullImage(c Client, image string) error {
	fmt.Printf("Pulling image: %s\n", image)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	res, err := c.ImagePull(ctx, image, types.ImagePullOptions{})
	if res != nil {
		defer res.Close()
		buf := bytes.Buffer{}
		buf.ReadFrom(res)
		zap.L().Sugar().Debug(buf.String())
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func CheckIfImgPresent(c Client, image string) (bool, error) {
	ctx, _ := DefaultCtx()
	filters := filters.NewArgs(
		filters.Arg("reference", image),
	)
	imgs, err := c.ImageList(ctx, types.ImageListOptions{
		Filters: filters,
	})
	if err != nil {
		return false, err
	}
	return len(imgs) > 0, nil
}

func getHostPort(
	containerPort int, containerJSON types.ContainerJSON,
) (uint, error) {
	natContianerPort, err := nat.NewPort("tcp", fmt.Sprint(containerPort))
	if err != nil {
		return uint(0), err
	}
	bindings, exists := containerJSON.NetworkSettings.Ports[natContianerPort]
	if exists {
		if len(bindings) > 0 {
			hostPort, err := strconv.Atoi(bindings[0].HostPort)
			if err != nil {
				return uint(0), err
			}
			return uint(hostPort), nil
		}
	}
	return uint(0), nil
}

func nodeIP(c Client, netID string, id uint) (string, error) {
	ctx, _ := DefaultCtx()
	networkResource, err := c.NetworkInspect(ctx, netID, types.NetworkInspectOptions{})
	if err != nil {
		return "", err
	}

	if len(networkResource.IPAM.Config) != 1 {
		return "", fmt.Errorf(
			"'%s' network config is corrupted",
			networkResource.Name,
		)
	}
	gatewayAddress := networkResource.IPAM.Config[0].Gateway
	octets := strings.Split(gatewayAddress, ".")
	if len(octets) != 4 {
		return "", fmt.Errorf(
			"invalid container IP addr: %s",
			gatewayAddress,
		)
	}
	lastOctet, err := strconv.ParseUint(octets[3], 10, 64)
	if err != nil {
		return "", err
	}
	octets[3] = fmt.Sprintf("%d", lastOctet+uint64(id)+1)
	return strings.Join(octets, "."), nil
}

func WrapIfConnErr(err error) error {
	if client.IsErrConnectionFailed(err) {
		msg := `Unable to connect to docker.
This can happen for a couple of reasons:
- The Docker daemon isn't running.
- You are running 'rpk container' as a user that cannot execute Docker commands.
- You have not installed Docker. Please follow the instructions at https://docs.docker.com/engine/install/ to install it and then try again.
`
		return errors.New(msg)
	}
	return err
}
