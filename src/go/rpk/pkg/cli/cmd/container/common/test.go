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
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
)

type MockClient struct {
	MockClose func() error

	MockSetConnection func()

	MockImagePull func(
		ctx context.Context,
		ref string,
	) error

	MockImageList func(
		ctx context.Context,
		options ImageListOptions,
	) ([]types.ImageSummary, error)

	MockContainerCreate func(
		ctx context.Context,
		config *ContainerConfig,
		hostConfig *ContainerHostConfig,
		networkingConfig *ContainerNetwork,
		containerName string,
	) (string, error)

	MockContainerStart func(
		ctx context.Context,
		containerID string,
	) error

	MockContainerStop func(
		ctx context.Context,
		containerID string,
		timeout *time.Duration,
	) error

	MockContainerList func(
		ctx context.Context,
		options types.ContainerListOptions,
	) ([]ContainerList, error)

	MockContainerInspect func(
		ctx context.Context,
		containerID string,
	) (ContainerInspect, error)

	MockContainerRemove func(
		ctx context.Context,
		containerID string,
		options types.ContainerRemoveOptions,
	) error

	MockNetworkCreate func(
		ctx context.Context,
		name string,
		options NetworkCreateOptions,
	) (types.NetworkCreateResponse, error)

	MockNetworkRemove func(
		ctx context.Context,
		id string,
	) error

	MockNetworkList func(
		ctx context.Context,
		options NetworkListOptions,
	) (NetworkList, error)

	MockNetworkInspect func(
		ctx context.Context,
		networkID string,
	) (NetworkInspect, error)

	MockIsErrNotFound func(err error) bool

	MockIsErrConnectionFailed func(err error) bool
}

func (c *MockClient) Close() error {
	if c.MockClose != nil {
		return c.MockClose()
	}
	return nil
}

func (c *MockClient) SetConnection() {
	if c.MockSetConnection != nil {
		c.MockSetConnection()
	}
}

func (c *MockClient) ContainerCreate(
	ctx context.Context,
	config *ContainerConfig,
	hostConfig *ContainerHostConfig,
	networkingConfig *ContainerNetwork,
	containerName string,
) (string, error) {
	if c.MockContainerCreate != nil {
		return c.MockContainerCreate(
			ctx,
			config,
			hostConfig,
			networkingConfig,
			containerName,
		)
	}
	return "", nil
}

func (c *MockClient) ImagePull(
	ctx context.Context, ref string,
) error {
	if c.MockImagePull != nil {
		return c.MockImagePull(ctx, ref)
	}
	return nil
}

func (c *MockClient) ImageList(
	ctx context.Context, options ImageListOptions,
) ([]types.ImageSummary, error) {
	if c.MockImageList != nil {
		return c.MockImageList(ctx, options)
	}
	return []types.ImageSummary{}, nil
}

func (c *MockClient) ContainerStart(
	ctx context.Context, containerID string,
) error {
	if c.MockContainerStart != nil {
		return c.MockContainerStart(
			ctx, containerID,
		)
	}
	return nil
}

func (c *MockClient) ContainerStop(
	ctx context.Context, containerID string, timeout *time.Duration,
) error {
	if c.MockContainerStop != nil {
		return c.MockContainerStop(ctx, containerID, timeout)
	}
	return nil
}

func (c *MockClient) ContainerList(
	ctx context.Context, options types.ContainerListOptions,
) ([]ContainerList, error) {
	if c.MockContainerList != nil {
		return c.MockContainerList(ctx, options)
	}
	return []ContainerList{}, nil
}

func (c *MockClient) ContainerInspect(
	ctx context.Context, containerID string,
) (ContainerInspect, error) {
	if c.MockContainerInspect != nil {
		return c.MockContainerInspect(ctx, containerID)
	}
	return MockContainerInspect(ctx, containerID)
}

func (c *MockClient) ContainerRemove(
	ctx context.Context, containerID string, options types.ContainerRemoveOptions,
) error {
	if c.MockContainerRemove != nil {
		return c.MockContainerRemove(ctx, containerID, options)
	}
	return nil
}

func (c *MockClient) NetworkCreate(
	ctx context.Context, name string, options NetworkCreateOptions,
) (types.NetworkCreateResponse, error) {
	if c.MockNetworkCreate != nil {
		return c.MockNetworkCreate(ctx, name, options)
	}
	return types.NetworkCreateResponse{}, nil
}

func (c *MockClient) NetworkRemove(ctx context.Context, name string) error {
	if c.MockNetworkRemove != nil {
		return c.MockNetworkRemove(ctx, name)
	}
	return nil
}

func (c *MockClient) NetworkList(
	ctx context.Context, options NetworkListOptions,
) (NetworkList, error) {
	if c.MockNetworkList != nil {
		return c.MockNetworkList(ctx, options)
	}
	return NetworkList{}, nil
}

func (c *MockClient) NetworkInspect(
	ctx context.Context, networkID string,
) (NetworkInspect, error) {
	if c.MockNetworkInspect != nil {
		return c.MockNetworkInspect(ctx, networkID)
	}
	return NetworkInspect{}, nil
}

func (c *MockClient) IsErrNotFound(err error) bool {
	if c.MockIsErrNotFound != nil {
		return c.MockIsErrNotFound(err)
	}
	return false
}

func (c *MockClient) IsErrConnectionFailed(err error) bool {
	if c.MockIsErrConnectionFailed != nil {
		return c.MockIsErrConnectionFailed(err)
	}
	return false
}

func MockContainerInspect(
	_ context.Context, _ string,
) (ContainerInspect, error) {
	kafkaNatPort := nat.Port("9093/tcp")
	rpcNatPort := nat.Port("33145/tcp")
	return ContainerInspect{
		Running: true,
		Status:  "Up, I guess?",
		Ports: map[nat.Port][]nat.PortBinding{
			kafkaNatPort: {{
				HostIP: "192.168.78.9", HostPort: "89080",
			}},
			rpcNatPort: {{
				HostIP: "192.168.78.9", HostPort: "89081",
			}},
		},
		Networks: []ContainerNetwork{
			{
				ID:       "redpanda",
				Name:     "redpanda",
				IPAdress: "172.24.1.2",
			},
		},
	}, nil
}
