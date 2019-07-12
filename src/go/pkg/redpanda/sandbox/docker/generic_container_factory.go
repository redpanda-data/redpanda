package docker

import (
	"fmt"
	"strconv"
	"vectorized/redpanda/sandbox/docker/labels"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
)

// NewGenericContainerFactroy creates a sandbox node
// container based on generic image i.e fedora:30
func NewGenericContainerFactroy(
	dockerClient *client.Client, installDir string,
) ContainerFactory {
	return &genericContainerFactory{
		dockerClient: dockerClient,
		installDir:   installDir,
		image:        "fedora:30",
	}
}

type genericContainerFactory struct {
	ContainerFactory
	dockerClient *client.Client
	image        string
	installDir   string
}

func (f *genericContainerFactory) CreateNodeContainer(
	cfg *NodeContainerCfg,
) error {
	ctx, cancel := CtxWithDefaultTimeout()
	defer cancel()
	images, err := f.dockerClient.ImageList(ctx,
		types.ImageListOptions{
			Filters: filters.NewArgs(
				filters.KeyValuePair{
					Key:   "reference",
					Value: f.image,
				},
			),
		})
	if err != nil {
		return err
	}
	if len(images) == 0 {
		log.Infof("Pulling '%s' docker image", f.image)
		ctx, cancel := CtxWithDefaultTimeout()
		defer cancel()
		_, err := f.dockerClient.ImagePull(
			ctx, f.image, types.ImagePullOptions{})
		if err != nil {
			return err
		}
	}
	log.Debugf("Creating container for node '%d' ", cfg.NodeID)

	rpcPort, err := nat.NewPort("tcp", strconv.Itoa(cfg.RPCPort))
	if err != nil {
		return err
	}
	containerConfig := container.Config{
		Image: f.image,
		Cmd: []string{
			"/redpanda/bin/redpanda",
			"--redpanda-cfg",
			"/rp/conf/redpanda.yaml",
		},
		Volumes: map[string]struct{}{
			"/redpanda": {},
			"/rp/data":  {},
			"/rp/conf":  {},
		},
		ExposedPorts: nat.PortSet{
			rpcPort: {},
		},
		Labels: map[string]string{
			labels.ClusterID: cfg.ClusterID,
			labels.NodeID:    strconv.Itoa(cfg.NodeID),
		},
	}

	hostConfig := container.HostConfig{
		Mounts: []mount.Mount{
			mount.Mount{
				Type:     mount.TypeBind,
				Target:   "/redpanda",
				Source:   f.installDir,
				ReadOnly: true,
			},
			mount.Mount{
				Type:     mount.TypeBind,
				Target:   "/rp/data",
				Source:   cfg.DataDir,
				ReadOnly: false,
			},
			mount.Mount{
				Type:     mount.TypeBind,
				Target:   "/rp/conf",
				Source:   cfg.ConfDir,
				ReadOnly: true,
			},
		},
		PortBindings: nat.PortMap{
			rpcPort: []nat.PortBinding{nat.PortBinding{}},
		},
		CapAdd: []string{"SYS_NICE"},
	}
	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			cfg.NetworkName: &network.EndpointSettings{
				IPAMConfig: &network.EndpointIPAMConfig{
					IPv4Address: cfg.ContainerIP,
				},
				Aliases: []string{fmt.Sprintf("rp-node-%d", cfg.NodeID)},
			},
		},
	}
	ctx, cancel = CtxWithDefaultTimeout()
	defer cancel()
	body, err := f.dockerClient.ContainerCreate(
		ctx, &containerConfig, &hostConfig, &networkConfig, "")
	log.Debugf("Container with id '%s' created", body.ID)
	return err
}
