// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
package docker

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	log "github.com/sirupsen/logrus"
)

var once sync.Once

type DockerClient struct {
	conn *client.Client
}

func (c *DockerClient) SetConnection() {
	once.Do(func() {
		dc, _ := client.NewClientWithOpts(client.FromEnv)
		c.conn = dc
	})
}

func (c *DockerClient) Close() error {
	return c.conn.Close()
}

func (c *DockerClient) ImagePull(
	ctx context.Context,
	ref string,
) error {
	res, err := c.conn.ImagePull(ctx, ref, types.ImagePullOptions{})
	if res != nil {
		defer res.Close()
		buf := bytes.Buffer{}
		buf.ReadFrom(res)
		log.Debug(buf.String())
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (c *DockerClient) ContainerList(ctx context.Context, options types.ContainerListOptions) ([]common.ContainerList, error) {
	containers, err := c.conn.ContainerList(ctx, options)
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

func (c *DockerClient) ContainerInspect(ctx context.Context, containerID string) (common.ContainerInspect, error) {
	info, err := c.conn.ContainerInspect(ctx, containerID)
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

	ci := common.ContainerInspect{
		Status:   info.State.Status,
		Running:  info.State.Running,
		ID:       info.ID,
		Networks: networks,
		Ports:    info.NetworkSettings.Ports,
	}

	return ci, nil
}

func (c *DockerClient) NetworkCreate(
	ctx context.Context,
	name string,
	options common.NetworkCreateOptions,
) (types.NetworkCreateResponse, error) {
	nc := types.NetworkCreate{
		Driver: *options.Driver,
		IPAM: &network.IPAM{
			Driver: "default",
			Config: []network.IPAMConfig{
				{
					Subnet:  options.Subnet,
					Gateway: options.Gateway,
				},
			},
		},
	}
	return c.conn.NetworkCreate(ctx, name, nc)
}

func (c *DockerClient) NetworkRemove(ctx context.Context, networkID string) error {
	return c.conn.NetworkRemove(ctx, networkID)
}

func (c *DockerClient) ContainerRemove(
	ctx context.Context,
	containerID string,
	options types.ContainerRemoveOptions,
) error {
	return c.conn.ContainerRemove(ctx, containerID, options)
}

func (c *DockerClient) ContainerCreate(
	ctx context.Context,
	config *common.ContainerConfig,
	hostConfig *common.ContainerHostConfig,
	networkingConfig *common.ContainerNetwork,
	containerName string,
) (string, error) {
	cc := &container.Config{
		Image:        config.Image,
		Hostname:     config.Hostname,
		Cmd:          config.Cmd,
		ExposedPorts: config.ExposedPorts,
		Labels:       config.Labels,
	}

	hc := &container.HostConfig{
		PortBindings: hostConfig.PortBindings,
	}

	nc := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			networkingConfig.ID: {
				IPAMConfig: &network.EndpointIPAMConfig{
					IPv4Address: networkingConfig.IPAdress,
				},
				Aliases: networkingConfig.Aliases,
			},
		},
	}
	resp, err := c.conn.ContainerCreate(ctx, cc, hc, nc, nil, containerName)
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

func (c *DockerClient) ContainerStart(
	ctx context.Context,
	containerID string,
) error {
	return c.conn.ContainerStart(ctx, containerID, types.ContainerStartOptions{})
}

func (c *DockerClient) ContainerStop(
	ctx context.Context,
	containerID string,
	timeout *time.Duration,
) error {
	return c.conn.ContainerStop(ctx, containerID, timeout)
}

func (c *DockerClient) ImageList(
	ctx context.Context,
	options common.ImageListOptions,
) ([]types.ImageSummary, error) {
	filters := filters.NewArgs(
		filters.Arg("reference", options.Ref),
	)

	return c.conn.ImageList(ctx, types.ImageListOptions{
		Filters: filters,
	})
}

func (c *DockerClient) NetworkInspect(
	ctx context.Context,
	networkID string,
) (common.NetworkInspect, error) {
	net, err := c.conn.NetworkInspect(ctx, networkID, types.NetworkInspectOptions{})
	if err != nil {
		return common.NetworkInspect{}, err
	}

	if len(net.IPAM.Config) != 1 {
		return common.NetworkInspect{}, fmt.Errorf(
			"'%s' network config is corrupted",
			net.Name,
		)
	}

	ni := common.NetworkInspect{
		Name:    net.Name,
		Gateway: net.IPAM.Config[0].Gateway,
	}

	return ni, nil
}

func (c *DockerClient) NetworkList(
	ctx context.Context,
	options common.NetworkListOptions,
) (common.NetworkList, error) {
	args := filters.NewArgs()
	args.Add("name", options.Name)

	nl, err := c.conn.NetworkList(ctx, types.NetworkListOptions{Filters: args})
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

func (*DockerClient) IsErrNotFound(err error) bool {
	return client.IsErrNotFound(err)
}

func (*DockerClient) IsErrConnectionFailed(err error) bool {
	return client.IsErrConnectionFailed(err)
}
