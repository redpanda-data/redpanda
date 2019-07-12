package sandbox

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"vectorized/redpanda"
	"vectorized/redpanda/sandbox/docker"
	"vectorized/redpanda/sandbox/docker/labels"
	"vectorized/utils"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	redpandaNetwork = "redpanda-net"
)

type Sandbox interface {
	Create(numberOfNodes int, containerFactory docker.ContainerFactory) error
	Start() error
	Stop() error
	Restart() error
	Destroy() error
	WipeRestart() error
	State() (*State, error)
	AddNode() error
	RemoveNode(nodeID int) error
	RestartNode(nodeID int) error
	WipeRestartNode(nodeID int) error
	StopNode(nodeID int) error
	StartNode(nodeID int) error
}

type sandbox struct {
	Sandbox
	fs           afero.Fs
	dockerClient *client.Client
	sandboxDir   string
}

func NewSandbox(
	fs afero.Fs, sandboxDir string, dockerClient *client.Client,
) Sandbox {
	return &sandbox{
		fs:           fs,
		sandboxDir:   sandboxDir,
		dockerClient: dockerClient,
	}
}

func (s *sandbox) Create(
	numberOfNodes int, containerFactory docker.ContainerFactory,
) error {
	log.Infof("Creating sandbox with '%d' nodes", numberOfNodes)
	if !utils.FileExists(s.fs, s.sandboxDir) {
		err := s.fs.MkdirAll(s.sandboxDir, 0755)
		if err != nil {
			return err
		}
		var nodes []Node
		var seedServers []*redpanda.SeedServer
		networkID, err := s.getSandboxNetworkID(true)
		if err != nil {
			return err
		}
		for i := 0; i < numberOfNodes; i++ {
			node := s.getNode(i)
			nodeIP, err := s.getNodeIPAddress(networkID, i)
			if err != nil {
				return err
			}
			err = node.Create(nodeIP, containerFactory)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			seedServer := &redpanda.SeedServer{
				Id:      i,
				Address: fmt.Sprintf("%s:%d", nodeIP, containerRPCPort),
			}
			seedServers = append(seedServers, seedServer)
		}

		for _, n := range nodes {
			n.Configure(seedServers)
		}

	} else {
		nodeIDs, err := s.getNodeIDs()
		if err != nil {
			return err
		}
		if len(nodeIDs) != numberOfNodes {
			return fmt.Errorf("Sandbox configuration with different"+
				" number of nodes already exists "+
				"in '%s' please destroy the sandbox first", s.sandboxDir)
		}
	}

	return s.Start()
}

func (s *sandbox) Start() error {
	log.Infof("Starting sandbox '%s'", s.sandboxDir)
	err := s.executeOnEachNode(Node.Start)
	if err != nil {
		return err
	}
	log.Infof("Sandbox '%s' started", s.sandboxDir)
	return nil
}

func (s *sandbox) Stop() error {
	log.Infof("Stopping sandbox '%s'", s.sandboxDir)
	err := s.executeOnEachNode(Node.Stop)
	if err != nil {
		return err
	}
	log.Infof("Sandbox '%s' stopped", s.sandboxDir)
	return nil
}

func (s *sandbox) Restart() error {
	err := s.Stop()
	if err != nil {
		return err
	}
	return s.Start()
}

func (s *sandbox) Destroy() error {
	log.Infof("Destroying sandbox '%s'", s.sandboxDir)
	err := s.executeOnEachNode(Node.Destroy)
	if err != nil {
		return err
	}
	err = s.fs.RemoveAll(s.sandboxDir)
	if err != nil {
		return err
	}
	networkID, err := s.getSandboxNetworkID(false)
	if err != nil {
		log.Warn(err)
	} else {
		ctx, cancel := docker.CtxWithDefaultTimeout()
		defer cancel()
		err = s.dockerClient.NetworkRemove(ctx, networkID)
		if err != nil {
			return err
		}
	}
	log.Infof("Sandbox '%s' destroyed", s.sandboxDir)
	return nil
}

func (s *sandbox) WipeRestart() error {
	log.Infof("Wiping sandbox '%s'", s.sandboxDir)
	err := s.Stop()
	if err != nil {
		return err
	}
	err = s.executeOnEachNode(Node.Wipe)
	err = s.Start()
	if err != nil {
		return err
	}
	log.Infof("Sandbox '%s' wipped", s.sandboxDir)
	return nil
}

func (s *sandbox) State() (*State, error) {
	nodeIDs, err := s.getNodeIDs()
	if err != nil {
		return nil, err
	}
	var nodeStates []*NodeState
	for _, nodeID := range nodeIDs {
		nodeState, err := s.getNode(nodeID).State()
		if err != nil {
			return nil, err
		}
		nodeStates = append(nodeStates, nodeState)
	}
	return &State{
		NodeStates: nodeStates,
	}, nil
}

func (s *sandbox) AddNode() error {
	//TODO: Figure out protocol of adding/remving nodes
	panic("not implemented")
}

func (s *sandbox) RemoveNode(nodeID int) error {
	//TODO: Figure out protocol of adding/remving nodes
	panic("not implemented")
}

func (s *sandbox) RestartNode(nodeID int) error {
	return s.singleNodeOp(nodeID, Node.Restart)
}

func (s *sandbox) StartNode(nodeID int) error {
	return s.singleNodeOp(nodeID, Node.Start)
}

func (s *sandbox) StopNode(nodeID int) error {
	return s.singleNodeOp(nodeID, Node.Stop)
}

func (s *sandbox) WipeRestartNode(nodeID int) error {
	err := s.StopNode(nodeID)
	if err != nil {
		return err
	}
	err = s.getNode(nodeID).Wipe()
	if err != nil {
		return err
	}
	return s.StartNode(nodeID)
}

func (s *sandbox) singleNodeOp(nodeID int, action func(Node) error) error {
	nodes, err := s.getNodeIDs()
	if err != nil {
		return err
	}
	if !utils.ContainsInt(nodes, nodeID) {
		return fmt.Errorf("Sandbox does not contain node %d", nodeID)
	}
	err = s.executeOnNode(nodeID, action)
	if err != nil {
		return err
	}
	return nil
}

func (s *sandbox) nodeDir(id int) string {
	return filepath.Join(s.sandboxDir, "nodes", fmt.Sprintf("n-%d", id))
}

func (s *sandbox) executeOnEachNode(action func(Node) error) error {
	nodes, err := s.getNodeIDs()
	if err != nil {
		return err
	}
	for _, nodeID := range nodes {
		err := s.executeOnNode(nodeID, action)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sandbox) executeOnNode(nodeID int, action func(Node) error) error {
	return action(s.getNode(nodeID))
}

func (s *sandbox) getNode(nodeID int) Node {
	return newNode(s.fs, s.nodeDir(nodeID),
		s.sandboxDir, nodeID, redpandaNetwork, s.dockerClient)
}

func (s *sandbox) getNodeIDs() ([]int, error) {
	log.Debugf("Getting sanbox node ids")
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	containers, err := s.dockerClient.ContainerList(
		ctx, types.ContainerListOptions{
			All: true,
			Filters: filters.NewArgs(filters.KeyValuePair{
				Key:   "label",
				Value: labels.ClusterIDFilter(s.sandboxDir),
			}),
		})
	if err != nil {
		return nil, err
	}
	var nodeIDs []int
	for _, container := range containers {
		nodeID, err := strconv.Atoi(container.Labels[labels.NodeID])
		if err != nil {
			return nil, err
		}
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs, nil
}

func (s *sandbox) getNodeIPAddress(networkID string, id int) (string, error) {
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	networkResource, err := s.dockerClient.NetworkInspect(
		ctx, networkID, types.NetworkInspectOptions{})
	if err != nil {
		return "", err
	}

	if len(networkResource.IPAM.Config) != 1 {
		return "", fmt.Errorf(
			"Configuration of %s network is corrupted", networkResource.Name)
	}
	gatewayAddress := networkResource.IPAM.Config[0].Gateway
	octets := strings.Split(gatewayAddress, ".")
	lastOctet, err := strconv.Atoi(octets[3])
	if err != nil {
		return "", err
	}
	octets[3] = strconv.Itoa(lastOctet + id + 1)
	return strings.Join(octets, "."), nil
}

func (s *sandbox) getSandboxNetworkID(create bool) (string, error) {
	log.Debugf("Getting sandbox network")
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	networks, err := s.dockerClient.NetworkList(ctx, types.NetworkListOptions{
		Filters: filters.NewArgs(filters.KeyValuePair{
			Key: "name", Value: redpandaNetwork}),
	})
	if err != nil {
		return "", err
	}

	if len(networks) == 0 {
		if create {
			log.Debugf("Sandbox network '%s' does not exists, creating...",
				redpandaNetwork)
			resp, err := s.dockerClient.NetworkCreate(
				ctx, redpandaNetwork, types.NetworkCreate{
					Driver:     "bridge",
					ConfigOnly: false,
					IPAM: &network.IPAM{
						Driver: "default",
						Config: []network.IPAMConfig{
							network.IPAMConfig{
								Subnet:  "172.24.1.0/24",
								Gateway: "172.24.1.1",
							},
						},
					},
				})
			if err != nil {
				return "", err
			}
			if resp.Warning != "" {
				log.Warn(resp.Warning)
			}
			return resp.ID, nil
		}
		return "", errors.New("Sandbox network does not exists")
	}
	return networks[0].ID, nil
}
