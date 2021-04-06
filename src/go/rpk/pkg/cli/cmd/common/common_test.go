// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common_test

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	ccommon "github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func TestDeduceBrokers(t *testing.T) {
	tests := []struct {
		name     string
		client   func() (ccommon.Client, error)
		config   func() (*config.Config, error)
		brokers  []string
		expected []string
	}{{
		name: "it should prioritize the flag over the config & containers",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		brokers:  []string{"192.168.34.12:9093"},
		expected: []string{"192.168.34.12:9093"},
	}, {
		name: "it should prioritize the local containers over the config",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		expected: []string{"127.0.0.1:89080"},
	}, {
		name: "it should fall back to the config if the docker client" +
			" can't be init'd",
		client: func() (ccommon.Client, error) {
			return nil, errors.New("The docker client can't be initialized")
		},
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Redpanda.KafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.25.88",
					Port:    1235,
				},
			}}
			return conf, nil
		},
		expected: []string{"192.168.25.88:1235"},
	}, {
		name: "it should fall back to the default addr if there's an" +
			" error reading the config",
		config: func() (*config.Config, error) {
			return nil, errors.New("The config file couldn't be read")
		},
		expected: []string{"127.0.0.1:9092"},
	}, {
		name: "it should prioritize the config over the default broker addr",
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Redpanda.KafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.25.87",
					Port:    1234,
				},
			}}
			return conf, nil
		},
		expected: []string{"192.168.25.87:1234"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			client := func() (ccommon.Client, error) {
				return &ccommon.MockClient{}, nil
			}
			config := func() (*config.Config, error) {
				return config.Default(), nil
			}
			brokersList := []string{}
			brokers := &brokersList

			if tt.client != nil {
				client = tt.client
			}
			if tt.config != nil {
				config = tt.config
			}
			if tt.brokers != nil {
				brokers = &tt.brokers
			}
			bs := common.DeduceBrokers(fs, client, config, brokers)()
			require.Exactly(st, tt.expected, bs)
		})
	}
}

func TestAddKafkaFlags(t *testing.T) {
	var (
		brokers    []string
		configFile string
	)
	command := func() *cobra.Command {
		parent := &cobra.Command{
			Use: "parent",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		child := &cobra.Command{
			Use: "child",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		parent.AddCommand(child)

		common.AddKafkaFlags(parent, &configFile, &brokers)
		return parent
	}

	cmd := command()
	cmd.SetArgs([]string{
		"--config", "arbitraryconfig.yaml",
		"--brokers", "192.168.72.22:9092,localhost:9092",
	})

	err := cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "arbitraryconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.22:9092", "localhost:9092"}, brokers)

	// The flags should be available for the children commands too
	cmd = command() // reset it.
	cmd.SetArgs([]string{
		"child", // so that it executes the child command
		"--config", "justaconfig.yaml",
		"--brokers", "192.168.72.23:9092",
		"--brokers", "mykafkahost:9093",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "justaconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.23:9092", "mykafkahost:9093"}, brokers)
}
