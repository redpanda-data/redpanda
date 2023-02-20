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

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

// Defines an interface with the functions from Docker's *client.Client that are
// used, to make it possible to test the code that uses it.
type Client interface {
	Close() error

	ImagePull(
		ctx context.Context,
		ref string,
		options types.ImagePullOptions,
	) (io.ReadCloser, error)

	ImageList(
		ctx context.Context,
		options types.ImageListOptions,
	) ([]types.ImageSummary, error)
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
		options types.ContainerStartOptions,
	) error

	ContainerStop(
		ctx context.Context,
		containerID string,
		options container.StopOptions,
	) error

	ContainerList(
		ctx context.Context,
		options types.ContainerListOptions,
	) ([]types.Container, error)

	ContainerInspect(
		ctx context.Context,
		containerID string,
	) (types.ContainerJSON, error)

	ContainerRemove(
		ctx context.Context,
		containerID string,
		options types.ContainerRemoveOptions,
	) error

	NetworkCreate(
		ctx context.Context,
		name string,
		options types.NetworkCreate,
	) (types.NetworkCreateResponse, error)

	NetworkRemove(ctx context.Context, networkID string) error

	NetworkList(
		ctx context.Context,
		options types.NetworkListOptions,
	) ([]types.NetworkResource, error)

	NetworkInspect(
		ctx context.Context,
		networkID string,
		options types.NetworkInspectOptions,
	) (types.NetworkResource, error)

	IsErrNotFound(err error) bool

	IsErrConnectionFailed(err error) bool
}

type dockerClient struct {
	*client.Client
}

func NewDockerClient() (Client, error) {
	c, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	c.NegotiateAPIVersion(context.Background())
	return &dockerClient{c}, nil
}

func (*dockerClient) IsErrNotFound(err error) bool {
	return client.IsErrNotFound(err)
}

func (*dockerClient) IsErrConnectionFailed(err error) bool {
	return client.IsErrConnectionFailed(err)
}
