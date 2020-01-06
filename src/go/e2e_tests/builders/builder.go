package builders

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"vectorized/e2e_tests/kafka"
	"vectorized/pkg/redpanda/sandbox"
	"vectorized/pkg/redpanda/sandbox/docker"

	"github.com/docker/docker/client"
	"github.com/spf13/afero"
)

type BrokerType int

const (
	RedpandaBroker = iota
	KafkaBroker
)

type SandboxBuilder interface {
	FromTarball(string) SandboxBuilder
	InDirectory(string) SandboxBuilder
	WithNodes(int) SandboxBuilder
	DestroyOldIfExists() SandboxBuilder
	WithBrokerType(BrokerType) SandboxBuilder
	Build() (sandbox.Sandbox, error)
}

func NewSandboxBuilder(fs afero.Fs) SandboxBuilder {
	return &sandboxBuilder{
		fs:                 fs,
		dir:                filepath.Join(os.TempDir(), "v_test_sandbox"),
		nodes:              1,
		destroyOldIfExists: false,
	}
}

type sandboxBuilder struct {
	fs                 afero.Fs
	nodes              int
	dir                string
	tarball            string
	brokerType         BrokerType
	destroyOldIfExists bool
}

func (b *sandboxBuilder) InDirectory(dir string) SandboxBuilder {
	b.dir = dir
	return b
}

func (b *sandboxBuilder) WithNodes(nodes int) SandboxBuilder {
	b.nodes = nodes
	return b
}

func (b *sandboxBuilder) DestroyOldIfExists() SandboxBuilder {
	b.destroyOldIfExists = true
	return b
}

func (b *sandboxBuilder) WithBrokerType(brokerType BrokerType) SandboxBuilder {
	b.brokerType = brokerType
	return b
}

func (b *sandboxBuilder) FromTarball(tarball string) SandboxBuilder {
	b.tarball = tarball
	return b
}

func (b *sandboxBuilder) Build() (sandbox.Sandbox, error) {
	if err := b.validateConfig(); err != nil {
		return nil, err
	}

	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	dockerClient.NegotiateAPIVersion(ctx)

	sandbox := sandbox.NewSandbox(b.fs, b.dir, dockerClient)
	if b.destroyOldIfExists {
		sandbox.Destroy()
	}

	var containerFactory docker.ContainerFactory
	switch b.brokerType {
	case RedpandaBroker:
		containerFactory = docker.NewTarballContainerFactroy(
			b.fs,
			dockerClient,
			b.tarball)
	case KafkaBroker:
		containerFactory = kafka.NewKafkaContainerFactroy(
			b.fs,
			dockerClient,
		)
	}

	err = sandbox.Create(b.nodes, containerFactory)

	if err != nil {
		return nil, err
	}
	return sandbox, nil
}

func (b *sandboxBuilder) validateConfig() error {
	switch b.brokerType {
	case RedpandaBroker:
		if b.tarball == "" {
			return errors.New("Tarball is required to create Redpanda sandbox")
		}
		if exists, _ := afero.Exists(b.fs, b.tarball); !exists {
			return fmt.Errorf("Redpanda tarball '%s' does not exists", b.tarball)
		}
	case KafkaBroker:
		if b.tarball != "" {
			return errors.New("Kafka based sandbox does " +
				"not use tarball parameter")
		}
		if b.nodes > 1 {
			return errors.New("Kafka based Sanbox supports currently only " +
				"single node configuration")
		}
	}
	return nil
}
