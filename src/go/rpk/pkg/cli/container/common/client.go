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
	"context"
	"io"
	"os"

	"github.com/docker/docker/api/types/image"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

// Client defines an interface with the functions from Docker's *client.Client
// that are used, to make it possible to test the code that uses it.
type Client interface {
	Close() error

	ImagePull(
		ctx context.Context,
		ref string,
		options image.PullOptions,
	) (io.ReadCloser, error)

	ImageList(
		ctx context.Context,
		options image.ListOptions,
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
		options container.StartOptions,
	) error

	ContainerStop(
		ctx context.Context,
		containerID string,
		options container.StopOptions,
	) error

	ContainerList(
		ctx context.Context,
		options container.ListOptions,
	) ([]types.Container, error)

	ContainerLogs(
		ctx context.Context,
		containerID string,
		options container.LogsOptions,
	) (io.ReadCloser, error)

	ContainerInspect(
		ctx context.Context,
		containerID string,
	) (types.ContainerJSON, error)

	ContainerRemove(
		ctx context.Context,
		containerID string,
		options container.RemoveOptions,
	) error

	NetworkCreate(
		ctx context.Context,
		name string,
		options network.CreateOptions,
	) (network.CreateResponse, error)

	NetworkRemove(ctx context.Context, networkID string) error

	NetworkList(
		ctx context.Context,
		options network.ListOptions,
	) ([]network.Inspect, error)

	NetworkInspect(
		ctx context.Context,
		networkID string,
		options network.InspectOptions,
	) (network.Inspect, error)

	IsErrNotFound(err error) bool

	IsErrConnectionFailed(err error) bool
}

type dockerClient struct {
	*client.Client
}

func NewDockerClient(ctx context.Context) (Client, error) {
	// First, we check if DOCKER_HOST is present or if /var/run/docker.sock
	// exists. If either of these conditions is met, we can safely start the
	// client using the pre-set client.FromEnv.
	dockerHostEnv := "DOCKER_HOST"
	dockerSocketDefaultPath := "/var/run/docker.sock"

	_, err := os.Stat(dockerSocketDefaultPath)
	socketNotPresent := err != nil && os.IsNotExist(err)

	var c *client.Client
	if _, ok := os.LookupEnv(dockerHostEnv); ok || !socketNotPresent {
		c, err = client.NewClientWithOpts(client.FromEnv)
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
	c.NegotiateAPIVersion(ctx)
	return &dockerClient{c}, nil
}

func (*dockerClient) IsErrNotFound(err error) bool {
	return client.IsErrNotFound(err)
}

func (*dockerClient) IsErrConnectionFailed(err error) bool {
	return client.IsErrConnectionFailed(err)
}
