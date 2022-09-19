package common

import (
	"context"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
)

type GenericClient interface {
	Close() error

	SetConnection()

	ImagePull(
		ctx context.Context,
		ref string,
	) error

	ImageList(
		ctx context.Context,
		options ImageListOptions,
	) ([]types.ImageSummary, error)

	ContainerCreate(
		ctx context.Context,
		config *ContainerConfig,
		hostConfig *ContainerHostConfig,
		networkingConfig *ContainerNetwork,
		containerName string,
	) (string, error)

	ContainerStart(
		ctx context.Context,
		containerID string,
	) error

	ContainerStop(
		ctx context.Context,
		containerID string,
		timeout *time.Duration,
	) error

	ContainerList(
		ctx context.Context,
		options types.ContainerListOptions,
	) ([]ContainerList, error)

	ContainerInspect(
		ctx context.Context,
		containerID string,
	) (ContainerInspect, error)

	ContainerRemove(
		ctx context.Context,
		containerID string,
		options types.ContainerRemoveOptions,
	) error

	NetworkCreate(
		ctx context.Context,
		name string,
		options NetworkCreateOptions,
	) (types.NetworkCreateResponse, error)

	NetworkRemove(ctx context.Context, networkID string) error

	NetworkList(
		ctx context.Context,
		options NetworkListOptions,
	) (NetworkList, error)

	NetworkInspect(
		ctx context.Context,
		networkID string,
	) (NetworkInspect, error)

	IsErrNotFound(err error) bool

	IsErrConnectionFailed(err error) bool
}

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

type ContainerInspect struct {
	Networks []ContainerNetwork
	Running  bool
	Status   string
	ID       string
	Ports    nat.PortMap
}

type ContainerNetwork struct {
	ID       string
	Name     string
	IPAdress string
	Aliases  []string
}

type ContainerConfig struct {
	Image        string
	Hostname     string
	Cmd          []string
	ExposedPorts nat.PortSet
	Labels       map[string]string
}

type ContainerHostConfig struct {
	PortBindings nat.PortMap
}

type ContainerList struct {
	ID     string
	Labels map[string]string
}

type ImageListOptions struct {
	Ref string
}

type NetworkCreateOptions struct {
	Driver  *string
	Gateway string
	Subnet  string
}

type NetworkInspect struct {
	Name    string
	Gateway string
}

type NetworkListOptions struct {
	Name string
}

type NetworkList struct {
	Name string
	ID   string
}
