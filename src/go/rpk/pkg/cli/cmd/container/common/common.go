package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"vectorized/pkg/config"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	redpandaNetwork   = "redpanda"
	registry          = "docker.io"
	redpandaImageBase = "vectorized/redpanda"
	redpandaImage     = registry + "/" + redpandaImageBase

	defaultDockerClientTimeout = 10 * time.Second
)

type NodeState struct {
	Status        string
	Running       bool
	ConfigFile    string
	HostRPCPort   uint
	HostKafkaPort uint
	ID            uint
	ContainerIP   string
	ContainerID   string
}

// Returns the container name for the given node ID.
func Name(nodeID uint) string {
	return fmt.Sprintf("rp-node-%d", nodeID)
}

func DefaultCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultDockerClientTimeout)
}

func ConfPath(nodeID uint) string {
	return filepath.Join(ConfDir(nodeID), "redpanda.yaml")
}

func DataDir(nodeID uint) string {
	return filepath.Join(NodeDir(nodeID), "data")
}

func ConfDir(nodeID uint) string {
	return filepath.Join(NodeDir(nodeID), "conf")
}

func NodeDir(nodeID uint) string {
	return filepath.Join(ClusterDir(), fmt.Sprintf("node-%d", nodeID))
}

func ClusterDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".rpk", "cluster")
}

func GetExistingNodes(fs afero.Fs) ([]uint, error) {
	nodeIDs := []uint{}
	nodeDirs, err := afero.ReadDir(fs, ClusterDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nodeIDs, nil
		}
		return nodeIDs, err
	}
	nameRegExp := regexp.MustCompile(`node-[\d]+`)
	for _, nodeDir := range nodeDirs {
		if nodeDir.IsDir() && nameRegExp.Match([]byte(nodeDir.Name())) {
			nameParts := strings.Split(nodeDir.Name(), "-")
			nodeIDStr := nameParts[len(nameParts)-1]
			nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
			if err != nil {
				return nodeIDs, fmt.Errorf(
					"Couldn't parse node ID '%s': %v",
					nodeIDStr,
					err,
				)
			}
			nodeIDs = append(nodeIDs, uint(nodeID))
		}
	}
	return nodeIDs, nil
}

func GetState(c Client, nodeID uint) (*NodeState, error) {
	ctx, _ := DefaultCtx()
	containerJSON, err := c.ContainerInspect(ctx, Name(nodeID))
	if err != nil {
		return nil, err
	}
	var ipAddress string
	network, exists := containerJSON.NetworkSettings.Networks[redpandaNetwork]
	if exists {
		ipAddress = network.IPAMConfig.IPv4Address
	}

	hostRPCPort, err := getHostPort(
		config.DefaultConfig().Redpanda.RPCServer.Port,
		containerJSON,
	)
	if err != nil {
		return nil, err
	}
	hostKafkaPort, err := getHostPort(
		config.DefaultConfig().Redpanda.KafkaApi.Port,
		containerJSON,
	)
	if err != nil {
		return nil, err
	}
	return &NodeState{
		Running:       containerJSON.State.Running,
		Status:        containerJSON.State.Status,
		ConfigFile:    ConfPath(nodeID),
		ContainerID:   containerJSON.ID,
		ContainerIP:   ipAddress,
		HostKafkaPort: hostKafkaPort,
		HostRPCPort:   hostRPCPort,
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

	if len(networks) == 0 {
		log.Debugf(
			"Docker network '%s' doesn't exist, creating.",
			redpandaNetwork,
		)
		resp, err := c.NetworkCreate(
			ctx, redpandaNetwork, types.NetworkCreate{
				Driver: "bridge",
				IPAM: &network.IPAM{
					Driver: "default",
					Config: []network.IPAMConfig{
						network.IPAMConfig{
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
			log.Debug(resp.Warning)
		}
		return resp.ID, nil
	}
	return networks[0].ID, nil
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
	fs afero.Fs, c Client, nodeID,
	kafkaPort, rpcPort uint, netID string,
) (*NodeState, error) {
	rPort, err := nat.NewPort(
		"tcp",
		strconv.Itoa(config.DefaultConfig().Redpanda.RPCServer.Port),
	)
	if err != nil {
		return nil, err
	}
	kPort, err := nat.NewPort(
		"tcp",
		strconv.Itoa(config.DefaultConfig().Redpanda.KafkaApi.Port),
	)
	if err != nil {
		return nil, err
	}
	hostname := Name(nodeID)
	containerConfig := container.Config{
		Image:    redpandaImageBase,
		Hostname: hostname,
		ExposedPorts: nat.PortSet{
			rPort: {},
			kPort: {},
		},
		Labels: map[string]string{
			"cluster-id": "redpanda",
			"node-id":    fmt.Sprint(nodeID),
		},
	}
	hostConfig := container.HostConfig{
		Mounts: []mount.Mount{
			mount.Mount{
				Type:   mount.TypeBind,
				Target: "/opt/redpanda/data",
				Source: DataDir(nodeID),
			},
			mount.Mount{
				Type:   mount.TypeBind,
				Target: "/etc/redpanda",
				Source: ConfDir(nodeID),
			},
		},
		PortBindings: nat.PortMap{
			rPort: []nat.PortBinding{nat.PortBinding{
				HostPort: fmt.Sprint(rpcPort),
			}},
			kPort: []nat.PortBinding{nat.PortBinding{
				HostPort: fmt.Sprint(kafkaPort),
			}},
		},
	}
	ip, err := nodeIP(c, netID, nodeID)
	if err != nil {
		return nil, err
	}
	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			redpandaNetwork: &network.EndpointSettings{
				IPAMConfig: &network.EndpointIPAMConfig{
					IPv4Address: ip,
				},
				Aliases: []string{hostname},
			},
		},
	}

	err = createNodeDirs(fs, nodeID)
	if err != nil {
		RemoveNodeDir(fs, nodeID)
		return nil, err
	}

	ctx, _ := DefaultCtx()
	container, err := c.ContainerCreate(
		ctx,
		&containerConfig,
		&hostConfig,
		&networkConfig,
		hostname,
	)
	if err != nil {
		RemoveNodeDir(fs, nodeID)
		return nil, err
	}
	return &NodeState{
		ConfigFile:    ConfPath(nodeID),
		HostKafkaPort: kafkaPort,
		ID:            nodeID,
		ContainerID:   container.ID,
		ContainerIP:   ip,
	}, nil
}

func PullImage(c Client) error {
	ctx, _ := DefaultCtx()
	res, err := c.ImagePull(ctx, redpandaImage, types.ImagePullOptions{})
	if res != nil {
		defer res.Close()
		buf := bytes.Buffer{}
		buf.ReadFrom(res)
		log.Debug(buf.String())
	}
	return err
}

func CheckIfImgPresent(c Client) (bool, error) {
	ctx, _ := DefaultCtx()
	filters := filters.NewArgs(
		filters.Arg("reference", redpandaImageBase),
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

func createNodeDirs(fs afero.Fs, nodeID uint) error {
	dir := DataDir(nodeID)
	// If it doesn't exist already, create a directory for the node's data.
	exists, err := afero.DirExists(fs, dir)
	if err != nil {
		return err
	}
	if !exists {
		err = fs.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}

	dir = ConfDir(nodeID)
	// If it doesn't exist already, create a directory for the node's config.
	exists, err = afero.DirExists(fs, dir)
	if err != nil {
		return err
	}
	if !exists {
		err = fs.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

func RemoveNodeDir(fs afero.Fs, nodeID uint) error {
	err := fs.RemoveAll(NodeDir(nodeID))
	if err != nil {
		log.Debugf(
			"Got an error while removing node %d's dir: %v",
			nodeID,
			err,
		)
	}
	return err
}

func WrapIfConnErr(err error) error {
	if client.IsErrConnectionFailed(err) {
		msg := `Couldn't connect to docker.
This can happen for a couple of reasons:
- The Docker daemon isn't running.
- You are running 'rpk container' as a user that can't execute Docker commands.
- You haven't installed Docker. Please follow the instructions at https://docs.docker.com/engine/install/ to install it and then try again.
`
		log.Debug(err)
		return errors.New(msg)
	}
	return err
}
