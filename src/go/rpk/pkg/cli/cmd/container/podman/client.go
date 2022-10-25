// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package podman

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	libnetypes "github.com/containers/common/libnetwork/types"
	"github.com/containers/podman/v4/pkg/bindings"
	"github.com/containers/podman/v4/pkg/bindings/containers"
	"github.com/containers/podman/v4/pkg/bindings/images"
	"github.com/containers/podman/v4/pkg/bindings/network"
	"github.com/containers/podman/v4/pkg/domain/entities"
	"github.com/containers/podman/v4/pkg/specgen"
	"github.com/containers/podman/v4/pkg/specgenutil"
	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
)

var (
	once sync.Once
)

type PodmanClient struct {
	conn context.Context
}

func (c *PodmanClient) SetConnection() {
	once.Do(func() {
		// "unix:/Users/${USER}/.local/share/containers/podman/machine/podman-machine-default/podman.sock"
		pc, err := bindings.NewConnection(context.Background(), os.Getenv("PODMAN_HOST"))
		if err != nil {
			fmt.Print(err)
		}
		c.conn = pc
	})
}

func (c *PodmanClient) Close() error {
	return nil
}

func (c *PodmanClient) ImagePull(
	ctx context.Context,
	ref string,
) error {
	_, err := images.Pull(c.conn, ref, &images.PullOptions{})
	return err
}

func (c *PodmanClient) ContainerList(ctx context.Context, options types.ContainerListOptions) ([]common.ContainerList, error) {
	filters := make(map[string][]string)
	for _, key := range options.Filters.Keys() {
		filters[key] = options.Filters.Get(key)
	}

	containers, err := containers.List(
		c.conn,
		&containers.ListOptions{
			All:     common.BoolPtr(options.All),
			Filters: filters,
		},
	)
	if err != nil {
		return []common.ContainerList{}, err
	}

	cls := []common.ContainerList{}
	for _, cont := range containers {
		cls = append(cls, common.ContainerList{
			ID:     cont.ID,
			Labels: cont.Labels,
		})
	}

	return cls, nil
}

func (c *PodmanClient) ContainerInspect(ctx context.Context, containerID string) (common.ContainerInspect, error) {
	info, err := containers.Inspect(c.conn, containerID, nil)
	if err != nil {
		return common.ContainerInspect{}, err
	}

	var networks []common.ContainerNetwork
	for name, settings := range info.NetworkSettings.Networks {
		networks = append(networks, common.ContainerNetwork{
			ID:       settings.NetworkID,
			Name:     name,
			IPAdress: settings.IPAddress,
		})
	}

	portmap := nat.PortMap{}
	for key, value := range info.NetworkSettings.Ports {
		var portbindings []nat.PortBinding
		for _, binding := range value {
			portbindings = append(portbindings, nat.PortBinding{
				HostIP:   binding.HostIP,
				HostPort: binding.HostPort,
			})
			portmap[nat.Port(key)] = portbindings
		}
	}

	ci := common.ContainerInspect{
		Status:   info.State.Status,
		Running:  info.State.Running,
		ID:       info.ID,
		Networks: networks,
		Ports:    portmap,
	}

	return ci, nil
}

func (c *PodmanClient) NetworkCreate(
	ctx context.Context,
	name string,
	options common.NetworkCreateOptions,
) (types.NetworkCreateResponse, error) {
	cidr, err := libnetypes.ParseCIDR(options.Subnet)
	if err != nil {
		return types.NetworkCreateResponse{}, err
	}

	info, err := network.Create(
		c.conn,
		&libnetypes.Network{
			Name:   name,
			Driver: *options.Driver,
			Subnets: []libnetypes.Subnet{
				{
					Gateway: net.ParseIP(options.Gateway),
					Subnet:  cidr,
				},
			},
		},
	)
	if err != nil {
		return types.NetworkCreateResponse{}, err
	}

	return types.NetworkCreateResponse{ID: info.ID}, nil
}

func (c *PodmanClient) NetworkRemove(ctx context.Context, networkID string) error {
	_, err := network.Remove(c.conn, networkID, nil)

	return err
}

func (c *PodmanClient) ContainerRemove(
	ctx context.Context,
	containerID string,
	options types.ContainerRemoveOptions,
) error {
	_, err := containers.Remove(c.conn, containerID, &containers.RemoveOptions{
		Volumes: common.BoolPtr(true),
		Force:   common.BoolPtr(true),
	})

	return err
}

func (c *PodmanClient) ContainerCreate(
	ctx context.Context,
	config *common.ContainerConfig,
	hostConfig *common.ContainerHostConfig,
	networkingConfig *common.ContainerNetwork,
	containerName string,
) (string, error) {
	var l []string
	for key, value := range config.Labels {
		l = append(l, strings.Join([]string{key, value}, "="))
	}

	var p []string
	for key, value := range config.ExposedPorts {
		if value != struct{}{} {
			return "", fmt.Errorf("Expose port configuration not supported")
		}
		p = append(p, string(key))
	}
	var pms []libnetypes.PortMapping
	for portproto, bindings := range hostConfig.PortBindings {
		for _, binding := range bindings {
			hp, err := nat.ParsePort(binding.HostPort)
			if err != nil {
				return "", err
			}
			pms = append(pms, libnetypes.PortMapping{
				ContainerPort: uint16(portproto.Int()),
				Protocol:      portproto.Proto(),
				HostIP:        binding.HostIP,
				HostPort:      uint16(hp),
			})
		}
	}

	cco := &entities.ContainerCreateOptions{
		Name:             containerName,
		Pull:             "never",
		Hostname:         config.Hostname,
		Label:            l,
		Expose:           p,
		MemorySwappiness: -1,
		Net: &entities.NetOptions{
			PublishPorts: pms,
			Networks: map[string]libnetypes.PerNetworkOptions{
				networkingConfig.ID: {
					StaticIPs: []net.IP{
						net.ParseIP(networkingConfig.IPAdress),
					},
					Aliases: networkingConfig.Aliases,
				},
			},
		},
	}

	spec := specgen.NewSpecGenerator(config.Image, false)
	if err := specgenutil.FillOutSpecGen(spec, cco, config.Cmd); err != nil {
		return "", err
	}

	r, err := containers.CreateWithSpec(c.conn, spec, nil)
	if err != nil {
		return "", err
	}

	return r.ID, nil
}

func (c *PodmanClient) ContainerStart(
	ctx context.Context,
	containerID string,
) error {
	return containers.Start(c.conn, containerID, nil)
}

func (c *PodmanClient) ContainerStop(
	ctx context.Context,
	containerID string,
	timeout *time.Duration,
) error {
	t := uint(*timeout)
	cso := containers.StopOptions{
		Timeout: &t,
	}
	return containers.Stop(c.conn, containerID, &cso)
}

func (c *PodmanClient) ImageList(
	ctx context.Context,
	options common.ImageListOptions,
) ([]types.ImageSummary, error) {
	ilo := images.ListOptions{
		Filters: map[string][]string{
			"reference": {options.Ref},
		},
	}
	is, err := images.List(c.conn, &ilo)
	if err != nil {
		return []types.ImageSummary{}, err
	}

	var imgs []types.ImageSummary
	for _, img := range is {
		imgs = append(imgs, types.ImageSummary{
			Containers:  int64(img.Containers),
			Created:     img.Created,
			ID:          img.ID,
			Labels:      img.Labels,
			ParentID:    img.ParentId,
			RepoDigests: img.RepoDigests,
			RepoTags:    img.RepoTags,
			SharedSize:  int64(img.SharedSize),
			Size:        img.VirtualSize,
			VirtualSize: img.VirtualSize,
		})
	}

	return imgs, nil
}

func (c *PodmanClient) NetworkInspect(
	ctx context.Context,
	networkID string,
) (common.NetworkInspect, error) {
	net, err := network.Inspect(c.conn, networkID, &network.InspectOptions{})
	if err != nil {
		return common.NetworkInspect{}, err
	}

	if len(net.Subnets) != 1 {
		return common.NetworkInspect{}, fmt.Errorf(
			"'%s' network config is corrupted",
			net.Name,
		)
	}
	ni := common.NetworkInspect{
		Name:    net.Name,
		Gateway: net.Subnets[0].Gateway.String(),
	}

	return ni, nil
}

func (c *PodmanClient) NetworkList(
	ctx context.Context,
	options common.NetworkListOptions,
) (common.NetworkList, error) {
	args := map[string][]string{
		"name": {
			options.Name,
		},
	}

	nl, err := network.List(
		c.conn,
		&network.ListOptions{
			Filters: args,
		},
	)
	if err != nil {
		return common.NetworkList{}, err
	}

	if len(nl) > 1 {
		return common.NetworkList{}, fmt.Errorf(
			"Expected only 1 network with name %s but got %d",
			options.Name,
			len(nl),
		)
	} else if len(nl) == 1 {
		return common.NetworkList{Name: nl[0].Name, ID: nl[0].ID}, nil
	}

	return common.NetworkList{}, nil
}

// TODO figure out correct error from podman
// defaulting to false will at least surface the error
func (*PodmanClient) IsErrNotFound(err error) bool {
	return false
}

// TODO figure out correct error from podman
// defaulting to false will at least surface the error
func (*PodmanClient) IsErrConnectionFailed(err error) bool {
	return false
}
