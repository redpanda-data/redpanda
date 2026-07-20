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
	"net/netip"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/api/types/network"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

type MockClient struct {
	MockClose func() error

	MockImagePull func(
		ctx context.Context,
		ref string,
		options ImagePullOptions,
	) (io.ReadCloser, error)

	MockImageList func(
		ctx context.Context,
		options ImageListOptions,
	) ([]image.Summary, error)

	MockContainerCreate func(
		ctx context.Context,
		config *container.Config,
		hostConfig *container.HostConfig,
		networkingConfig *network.NetworkingConfig,
		platform *specs.Platform,
		containerName string,
	) (container.CreateResponse, error)

	MockContainerStart func(
		ctx context.Context,
		containerID string,
		options ContainerStartOptions,
	) error

	MockContainerStop func(
		ctx context.Context,
		containerID string,
		options ContainerStopOptions,
	) error

	MockContainerList func(
		ctx context.Context,
		options ContainerListOptions,
	) ([]container.Summary, error)

	MockContainerInspect func(
		ctx context.Context,
		containerID string,
	) (container.InspectResponse, error)

	MockContainerRemove func(
		ctx context.Context,
		containerID string,
		options ContainerRemoveOptions,
	) error

	MockNetworkCreate func(
		ctx context.Context,
		name string,
		options NetworkCreateOptions,
	) (network.CreateResponse, error)

	MockNetworkRemove func(
		ctx context.Context,
		name string,
	) error

	MockNetworkList func(
		ctx context.Context,
		options NetworkListOptions,
	) ([]network.Summary, error)

	MockNetworkInspect func(
		ctx context.Context,
		networkID string,
		options NetworkInspectOptions,
	) (network.Inspect, error)

	MockIsErrNotFound func(err error) bool

	MockIsErrConnectionFailed func(err error) bool
}

func (c *MockClient) Close() error {
	if c.MockClose != nil {
		return c.MockClose()
	}
	return nil
}

func (c *MockClient) ContainerCreate(
	ctx context.Context,
	config *container.Config,
	hostConfig *container.HostConfig,
	networkingConfig *network.NetworkingConfig,
	platform *specs.Platform,
	containerName string,
) (container.CreateResponse, error) {
	if c.MockContainerCreate != nil {
		return c.MockContainerCreate(
			ctx,
			config,
			hostConfig,
			networkingConfig,
			platform,
			containerName,
		)
	}
	return container.CreateResponse{}, nil
}

func (c *MockClient) ImagePull(
	ctx context.Context, ref string, options ImagePullOptions,
) (io.ReadCloser, error) {
	if c.MockImagePull != nil {
		return c.MockImagePull(ctx, ref, options)
	}
	return nil, nil
}

func (c *MockClient) ImageList(
	ctx context.Context, options ImageListOptions,
) ([]image.Summary, error) {
	if c.MockImageList != nil {
		return c.MockImageList(ctx, options)
	}
	return []image.Summary{}, nil
}

func (c *MockClient) ContainerStart(
	ctx context.Context, containerID string, options ContainerStartOptions,
) error {
	if c.MockContainerStart != nil {
		return c.MockContainerStart(
			ctx, containerID, options,
		)
	}
	return nil
}

func (c *MockClient) ContainerStop(
	ctx context.Context, containerID string, options ContainerStopOptions,
) error {
	if c.MockContainerStop != nil {
		return c.MockContainerStop(ctx, containerID, options)
	}
	return nil
}

func (c *MockClient) ContainerList(
	ctx context.Context, options ContainerListOptions,
) ([]container.Summary, error) {
	if c.MockContainerList != nil {
		return c.MockContainerList(ctx, options)
	}
	return []container.Summary{}, nil
}

func (c *MockClient) ContainerInspect(
	ctx context.Context, containerID string,
) (container.InspectResponse, error) {
	if c.MockContainerInspect != nil {
		return c.MockContainerInspect(ctx, containerID)
	}
	return MockContainerInspect(ctx, containerID)
}

func (c *MockClient) ContainerRemove(
	ctx context.Context, containerID string, options ContainerRemoveOptions,
) error {
	if c.MockContainerRemove != nil {
		return c.MockContainerRemove(ctx, containerID, options)
	}
	return nil
}

func (c *MockClient) NetworkCreate(
	ctx context.Context, name string, options NetworkCreateOptions,
) (network.CreateResponse, error) {
	if c.MockNetworkCreate != nil {
		return c.MockNetworkCreate(ctx, name, options)
	}
	return network.CreateResponse{}, nil
}

func (c *MockClient) NetworkRemove(ctx context.Context, name string) error {
	if c.MockNetworkRemove != nil {
		return c.MockNetworkRemove(ctx, name)
	}
	return nil
}

func (c *MockClient) NetworkList(
	ctx context.Context, options NetworkListOptions,
) ([]network.Summary, error) {
	if c.MockNetworkList != nil {
		return c.MockNetworkList(ctx, options)
	}
	return []network.Summary{}, nil
}

func (c *MockClient) NetworkInspect(
	ctx context.Context, networkID string, options NetworkInspectOptions,
) (network.Inspect, error) {
	if c.MockNetworkInspect != nil {
		return c.MockNetworkInspect(ctx, networkID, options)
	}
	return network.Inspect{}, nil
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
) (container.InspectResponse, error) {
	kafkaPort := network.MustParsePort("9093/tcp")
	rpcPort := network.MustParsePort("33145/tcp")
	return container.InspectResponse{
		State: &container.State{
			Running: true,
			Status:  "Up, I guess?",
		},
		NetworkSettings: &container.NetworkSettings{
			Ports: network.PortMap{
				kafkaPort: {{
					HostIP: netip.MustParseAddr("192.168.78.9"), HostPort: "89080",
				}},
				rpcPort: {{
					HostIP: netip.MustParseAddr("192.168.78.9"), HostPort: "89081",
				}},
			},
			Networks: map[string]*network.EndpointSettings{
				"redpanda": {
					IPAMConfig: &network.EndpointIPAMConfig{
						IPv4Address: netip.MustParseAddr("172.24.1.2"),
					},
				},
			},
		},
	}, nil
}
