package sandbox

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/redpanda/sandbox/docker"
	"vectorized/pkg/redpanda/sandbox/docker/labels"
	"vectorized/pkg/utils"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	containerRPCPort   = 9457
	containerKafkaPort = 9092
)

type Node interface {
	Start() error
	Stop() error
	Restart() error
	Destroy() error
	WipeRestart() error
	Wipe() error
	Logs(numberOfLines int, follow bool, timestamps bool) (io.ReadCloser, error)
	Configure(seedServers []*redpanda.SeedServer) error
	Create(containerIP string, containerFactory docker.ContainerFactory) error
	State() (*NodeState, error)
}

type node struct {
	Node
	fs           afero.Fs
	nodeDir      string
	id           int
	clusterID    string
	networkName  string
	dockerClient *client.Client
}

func newNode(
	fs afero.Fs,
	nodeDir string,
	clusterID string,
	nodeID int,
	networkName string,
	dockerClient *client.Client,
) Node {
	return &node{
		fs:           fs,
		nodeDir:      nodeDir,
		clusterID:    clusterID,
		id:           nodeID,
		networkName:  networkName,
		dockerClient: dockerClient,
	}
}

func (n *node) Create(
	containerIP string, containerFactory docker.ContainerFactory,
) error {
	if !utils.FileExists(n.fs, n.dataDir()) {
		log.Debugf("Creating node in '%s'", n.nodeDir)
		n.fs.MkdirAll(n.dataDir(), 0755)
		n.fs.MkdirAll(n.confDir(), 0755)
		containerConfig := docker.NodeContainerCfg{
			DataDir:     n.dataDir(),
			ConfDir:     n.confDir(),
			RPCPort:     containerRPCPort,
			KafkaPort:   containerKafkaPort,
			NodeID:      n.id,
			ContainerIP: containerIP,
			NetworkName: n.networkName,
			ClusterID:   n.clusterID,
		}
		err := containerFactory.CreateNodeContainer(&containerConfig)
		if err != nil {
			return err
		}
		return nil
	}

	log.Infof("Node already exists in '%s'", n.nodeDir)
	return nil
}

func (n *node) Configure(seedServers []*redpanda.SeedServer) error {
	log.Debugf("Configuring node %d", n.id)
	return n.updateNodeConfig(n.id, "0.0.0.0", containerRPCPort, containerKafkaPort, seedServers)
}

func (n *node) Start() error {
	log.Infof("Starting node '%s'", n.nodeDir)
	state, err := n.getState()
	if err != nil {
		return err
	}
	if state.Running {
		log.Debug("Node is already running")
		return nil
	}
	log.Debugf("Starting container '%s'", state.ContainerID)
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	err = n.dockerClient.ContainerStart(
		ctx, state.ContainerID, types.ContainerStartOptions{})
	if err != nil {
		return err
	}
	log.Infof("Node '%s' started", n.nodeDir)
	return nil
}

func (n *node) Stop() error {
	log.Infof("Stopping node '%s'", n.nodeDir)
	state, err := n.getState()
	if err != nil {
		return err
	}
	if !state.Running {
		log.Debugf("Node is already stopped")
		return nil
	}
	timeout := 5 * time.Second
	log.Debugf("Stopping container '%s'", state.ContainerID)
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	err = n.dockerClient.ContainerStop(
		ctx, state.ContainerID, &timeout)
	if err != nil {
		return err
	}
	log.Infof("Node '%s' stopped", n.nodeDir)
	return nil
}

func (n *node) Restart() error {
	err := n.Stop()
	if err != nil {
		return err
	}
	return n.Start()
}

func (n *node) Destroy() error {
	log.Infof("Destroying node '%s'", n.nodeDir)
	err := n.Stop()
	if err != nil {
		log.Warn(err)
	}
	containerID, err := n.getContainerID()
	if err != nil {
		return err
	}
	log.Debugf("Removing container '%s'", containerID)
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	err = n.dockerClient.ContainerRemove(
		ctx, containerID, types.ContainerRemoveOptions{})
	if err != nil {
		log.Warn(err)
	}

	err = n.fs.RemoveAll(n.nodeDir)
	if err != nil {
		return err
	}
	log.Infof("Node '%s' destroyed", n.nodeDir)
	return nil
}

func (n *node) WipeRestart() error {
	err := n.Stop()
	if err != nil {
		return err
	}
	err = n.Wipe()
	if err != nil {
		return err
	}
	return n.Start()
}

func (n *node) Wipe() error {
	log.Infof("Wiping node '%s'", n.nodeDir)
	return n.fs.RemoveAll(n.dataDir())
}

func (n *node) State() (*NodeState, error) {
	return n.getState()
}

func (n *node) Logs(
	numberOfLines int, follow bool, timestamps bool,
) (io.ReadCloser, error) {
	containerID, err := n.getContainerID()
	if err != nil {
		return nil, err
	}
	var tail = "all"
	if numberOfLines != 0 {
		tail = fmt.Sprint(numberOfLines)
	}
	return n.dockerClient.ContainerLogs(context.Background(), containerID,
		types.ContainerLogsOptions{
			Timestamps: timestamps,
			ShowStderr: true,
			ShowStdout: true,
			Follow:     follow,
			Tail:       tail,
		})
}

func (n *node) updateNodeConfig(
	id int,
	address string,
	rpcPort int,
	kafkaPort int,
	seedServers []*redpanda.SeedServer,
) error {
	configPath := n.configPath()
	log.Debugf("Updating node config in %s", configPath)
	config := redpanda.Config{
		Redpanda: &redpanda.RedpandaConfig{
			Directory: "/opt/redpanda/data",
			KafkaApi: redpanda.SocketAddress{
				Port:    kafkaPort,
				Address: address,
			},
			RPCServer: redpanda.SocketAddress{
				Port:    rpcPort,
				Address: address,
			},
			Id:          id,
			SeedServers: seedServers,
		},
	}
	return redpanda.WriteConfig(&config, n.fs, configPath)
}

func (n *node) getState() (*NodeState, error) {
	log.Debug("Getting node state")
	containerID, err := n.getContainerID()
	if err != nil {
		return nil, err
	}
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	containerJSON, err := n.dockerClient.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}
	var ipAddress string
	if network, exists := containerJSON.NetworkSettings.Networks[n.networkName]; exists {
		ipAddress = network.IPAMConfig.IPv4Address
	}

	hostRPCPort, err := getHostPort(containerRPCPort, containerJSON)
	if err != nil {
		return nil, err
	}
	hostKafkaPort, err := getHostPort(containerKafkaPort, containerJSON)
	if err != nil {
		return nil, err
	}
	return &NodeState{
		Running:       containerJSON.State.Running,
		Status:        containerJSON.State.Status,
		NodeDir:       n.nodeDir,
		ContainerID:   containerID,
		ContainerIP:   ipAddress,
		RPCPort:       containerRPCPort,
		KafkaPort:     containerKafkaPort,
		HostKafkaPort: hostKafkaPort,
		HostRPCPort:   hostRPCPort,
		ID:            n.id,
	}, nil
}

func getHostPort(
	containerPort int, containerJSON types.ContainerJSON,
) (int, error) {
	natContianerPort, err := nat.NewPort("tcp", fmt.Sprint(containerPort))
	if err != nil {
		return 0, err
	}
	if portBindings, exist := containerJSON.NetworkSettings.Ports[natContianerPort]; exist {
		if len(portBindings) > 0 {
			hostPort, err := strconv.Atoi(portBindings[0].HostPort)
			if err != nil {
				return 0, err
			}
			return hostPort, nil
		}
	}
	return 0, nil
}

func (n *node) getContainerID() (string, error) {
	log.Debugf("Getting node '%d' container id", n.id)
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	containers, err := n.dockerClient.ContainerList(ctx,
		types.ContainerListOptions{
			All: true,
			Filters: filters.NewArgs(filters.KeyValuePair{
				Key:   "label",
				Value: labels.NodeIDFilter(n.id),
			},
				filters.KeyValuePair{
					Key:   "label",
					Value: labels.ClusterIDFilter(n.clusterID),
				},
			),
		})
	if err != nil {
		return "", err
	}
	if len(containers) == 0 {
		log.Warnf("Container for node %d does not exists", n.id)
		return "", nil
	}
	return containers[0].ID, nil
}

func (n *node) dataDir() string {
	return filepath.Join(n.nodeDir, "data")
}

func (n *node) confDir() string {
	return filepath.Join(n.nodeDir, "conf")
}

func (n *node) configPath() string {
	return filepath.Join(n.confDir(), "redpanda.yaml")
}
