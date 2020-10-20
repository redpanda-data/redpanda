package common

import (
	"context"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
)

// Defines an interface with the functions from Docker's *client.Client that are
// used, to make it possible to test the code that uses it.
type Client interface {
	Close() error

	ContainerCreate(
		ctx context.Context,
		config *container.Config,
		hostConfig *container.HostConfig,
		networkingConfig *network.NetworkingConfig,
		containerName string,
	) (container.ContainerCreateCreatedBody, error)

	ContainerStart(
		ctx context.Context,
		containerID string,
		options types.ContainerStartOptions,
	) error

	ContainerStop(
		ctx context.Context,
		containerID string,
		timeout *time.Duration,
	) error

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
	) (types.NetworkResource, error)

	IsErrNotFound(err error) bool
}

type dockerClient struct {
	*client.Client
}

func NewDockerClient() (Client, error) {
	c, err := client.NewEnvClient()
	if err != nil {
		return nil, err
	}
	return &dockerClient{c}, nil
}

func (_ *dockerClient) IsErrNotFound(err error) bool {
	return client.IsErrNotFound(err)
}
