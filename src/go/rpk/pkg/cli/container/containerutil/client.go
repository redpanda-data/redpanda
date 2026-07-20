// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package containerutil

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

// Client defines an interface with the functions from Docker's *client.Client
// that are used, to make it possible to test the code that uses it.
type Client interface {
	Close() error

	ImagePull(
		ctx context.Context,
		ref string,
		options ImagePullOptions,
	) (io.ReadCloser, error)

	ImageList(
		ctx context.Context,
		options ImageListOptions,
	) ([]image.Summary, error)
	ContainerCreate(
		ctx context.Context,
		config *container.Config,
		hostConfig *container.HostConfig,
		networkingConfig *network.NetworkingConfig,
		platform *specs.Platform,
		containerName string,
	) (container.CreateResponse, error)

	ContainerStart(
		ctx context.Context,
		containerID string,
		options ContainerStartOptions,
	) error

	ContainerStop(
		ctx context.Context,
		containerID string,
		options ContainerStopOptions,
	) error

	ContainerList(
		ctx context.Context,
		options ContainerListOptions,
	) ([]container.Summary, error)

	ContainerLogs(
		ctx context.Context,
		containerID string,
		options ContainerLogsOptions,
	) (io.ReadCloser, error)

	ContainerInspect(
		ctx context.Context,
		containerID string,
	) (container.InspectResponse, error)

	ContainerRemove(
		ctx context.Context,
		containerID string,
		options ContainerRemoveOptions,
	) error

	NetworkCreate(
		ctx context.Context,
		name string,
		options NetworkCreateOptions,
	) (network.CreateResponse, error)

	NetworkRemove(ctx context.Context, networkID string) error

	NetworkList(
		ctx context.Context,
		options NetworkListOptions,
	) ([]network.Summary, error)

	NetworkInspect(
		ctx context.Context,
		networkID string,
		options NetworkInspectOptions,
	) (network.Inspect, error)

	IsErrNotFound(err error) bool

	IsErrConnectionFailed(err error) bool
}

type dockerClient struct {
	*client.Client
}

type (
	ContainerListOptions   = client.ContainerListOptions
	ContainerLogsOptions   = client.ContainerLogsOptions
	ContainerRemoveOptions = client.ContainerRemoveOptions
	ContainerStartOptions  = client.ContainerStartOptions
	ContainerStopOptions   = client.ContainerStopOptions
	ImageListOptions       = client.ImageListOptions
	ImagePullOptions       = client.ImagePullOptions
	NetworkCreateOptions   = client.NetworkCreateOptions
	NetworkInspectOptions  = client.NetworkInspectOptions
	NetworkListOptions     = client.NetworkListOptions
)

func (c *dockerClient) ImagePull(
	ctx context.Context, ref string, options ImagePullOptions,
) (io.ReadCloser, error) {
	return c.Client.ImagePull(ctx, ref, options)
}

func (c *dockerClient) ImageList(
	ctx context.Context, options ImageListOptions,
) ([]image.Summary, error) {
	result, err := c.Client.ImageList(ctx, options)
	return result.Items, err
}

func (c *dockerClient) ContainerCreate(
	ctx context.Context,
	config *container.Config,
	hostConfig *container.HostConfig,
	networkingConfig *network.NetworkingConfig,
	platform *specs.Platform,
	containerName string,
) (container.CreateResponse, error) {
	result, err := c.Client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:           config,
		HostConfig:       hostConfig,
		NetworkingConfig: networkingConfig,
		Platform:         platform,
		Name:             containerName,
	})
	return container.CreateResponse{ID: result.ID, Warnings: result.Warnings}, err
}

func (c *dockerClient) ContainerStart(
	ctx context.Context, containerID string, options ContainerStartOptions,
) error {
	_, err := c.Client.ContainerStart(ctx, containerID, options)
	return err
}

func (c *dockerClient) ContainerStop(
	ctx context.Context, containerID string, options ContainerStopOptions,
) error {
	_, err := c.Client.ContainerStop(ctx, containerID, options)
	return err
}

func (c *dockerClient) ContainerList(
	ctx context.Context, options ContainerListOptions,
) ([]container.Summary, error) {
	result, err := c.Client.ContainerList(ctx, options)
	return result.Items, err
}

func (c *dockerClient) ContainerLogs(
	ctx context.Context, containerID string, options ContainerLogsOptions,
) (io.ReadCloser, error) {
	return c.Client.ContainerLogs(ctx, containerID, options)
}

func (c *dockerClient) ContainerInspect(
	ctx context.Context, containerID string,
) (container.InspectResponse, error) {
	result, err := c.Client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	return result.Container, err
}

func (c *dockerClient) ContainerRemove(
	ctx context.Context, containerID string, options ContainerRemoveOptions,
) error {
	_, err := c.Client.ContainerRemove(ctx, containerID, options)
	return err
}

func (c *dockerClient) NetworkCreate(
	ctx context.Context, name string, options NetworkCreateOptions,
) (network.CreateResponse, error) {
	result, err := c.Client.NetworkCreate(ctx, name, options)
	return network.CreateResponse{ID: result.ID, Warning: strings.Join(result.Warning, "; ")}, err
}

func (c *dockerClient) NetworkRemove(ctx context.Context, networkID string) error {
	_, err := c.Client.NetworkRemove(ctx, networkID, client.NetworkRemoveOptions{})
	return err
}

func (c *dockerClient) NetworkList(
	ctx context.Context, options NetworkListOptions,
) ([]network.Summary, error) {
	result, err := c.Client.NetworkList(ctx, options)
	return result.Items, err
}

func (c *dockerClient) NetworkInspect(
	ctx context.Context, networkID string, options NetworkInspectOptions,
) (network.Inspect, error) {
	result, err := c.Client.NetworkInspect(ctx, networkID, options)
	return result.Network, err
}

func NewDockerClient(_ context.Context) (Client, error) {
	// First, we check if DOCKER_HOST is present or if /var/run/docker.sock
	// exists. If either of these conditions is met, we can safely start the
	// client using the pre-set client.FromEnv.
	dockerHostEnv := "DOCKER_HOST"
	dockerSocketDefaultPath := "/var/run/docker.sock"

	_, err := os.Stat(dockerSocketDefaultPath)
	socketNotPresent := err != nil && os.IsNotExist(err)

	var c *client.Client
	if _, ok := os.LookupEnv(dockerHostEnv); ok || !socketNotPresent {
		c, err = client.New(client.FromEnv)
		if err != nil {
			return nil, err
		}
	} else {
		// If we don't have either the Docker host environment variable or the
		// socket in the default location, we must search for the socket in the
		// Docker context.
		c, err = clientFromDockerContext()
		if err != nil {
			return nil, err
		}
	}
	return &dockerClient{c}, nil
}

func (*dockerClient) IsErrNotFound(err error) bool {
	return errdefs.IsNotFound(err)
}

func (*dockerClient) IsErrConnectionFailed(err error) bool {
	return client.IsErrConnectionFailed(err)
}
