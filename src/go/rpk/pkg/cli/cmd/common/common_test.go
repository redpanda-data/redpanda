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
			bs := common.DeduceBrokers(client, config, brokers)()
			require.Exactly(st, tt.expected, bs)
		})
	}
}

func TestAddKafkaFlags(t *testing.T) {
	var (
		brokers        []string
		configFile     string
		user           string
		password       string
		mechanism      string
		certFile       string
		keyFile        string
		truststoreFile string
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

		common.AddKafkaFlags(
			parent,
			&configFile,
			&user,
			&password,
			&mechanism,
			&certFile,
			&keyFile,
			&truststoreFile,
			&brokers,
		)
		return parent
	}

	cmd := command()
	cmd.SetArgs([]string{
		"--config", "arbitraryconfig.yaml",
		"--brokers", "192.168.72.22:9092,localhost:9092",
		"--user", "david",
		"--password", "verysecrethaha",
		"--sasl-mechanism", "some-mechanism",
		"--tls-cert", "cert.pem",
		"--tls-key", "key.pem",
		"--tls-truststore", "truststore.pem",
	})

	err := cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "arbitraryconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.22:9092", "localhost:9092"}, brokers)
	require.Exactly(t, "david", user)
	require.Exactly(t, "verysecrethaha", password)
	require.Exactly(t, "some-mechanism", mechanism)
	require.Exactly(t, "cert.pem", certFile)
	require.Exactly(t, "key.pem", keyFile)
	require.Exactly(t, "truststore.pem", truststoreFile)

	// The flags should be available for the children commands too
	cmd = command() // reset it.
	cmd.SetArgs([]string{
		"child", // so that it executes the child command
		"--config", "justaconfig.yaml",
		"--brokers", "192.168.72.23:9092",
		"--brokers", "mykafkahost:9093",
		"--user", "juan",
		"--password", "sosecure",
		"--sasl-mechanism", "whatevs",
		"--tls-cert", "cert1.pem",
		"--tls-key", "key1.pem",
		"--tls-truststore", "truststore1.pem",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "justaconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.23:9092", "mykafkahost:9093"}, brokers)
	require.Exactly(t, "juan", user)
	require.Exactly(t, "sosecure", password)
	require.Exactly(t, "whatevs", mechanism)
	require.Exactly(t, "cert1.pem", certFile)
	require.Exactly(t, "key1.pem", keyFile)
	require.Exactly(t, "truststore1.pem", truststoreFile)
}

func TestKafkaAuthConfig(t *testing.T) {
	tests := []struct {
		name           string
		user           string
		password       string
		mechanism      string
		expected       *config.SCRAM
		expectedErrMsg string
	}{{
		name:           "it should fail if user is empty",
		password:       "somethingsecure",
		expectedErrMsg: "empty user. Pass --user to set a value.",
	}, {
		name:           "it should fail if password is empty",
		user:           "usuario",
		expectedErrMsg: "empty password. Pass --password to set a value.",
	}, {
		name:           "it should fail if both user and password are empty",
		expectedErrMsg: common.ErrNoCredentials.Error(),
	}, {
		name:      "it should fail if the mechanism isn't supported",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "super-crypto-3000",
		expectedErrMsg: "unsupported mechanism 'super-crypto-3000'. Pass --sasl-mechanism to set a value." +
			" Supported: SCRAM-SHA-256, SCRAM-SHA-512.",
	}, {
		name:      "it should support SCRAM-SHA-256",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-256",
		expected:  &config.SCRAM{User: "usuario", Password: "contraseño", Type: "SCRAM-SHA-256"},
	}, {
		name:      "it should support SCRAM-SHA-512",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-512",
		expected:  &config.SCRAM{User: "usuario", Password: "contraseño", Type: "SCRAM-SHA-512"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			closure := common.KafkaAuthConfig(&tt.user, &tt.password, &tt.mechanism)
			res, err := closure()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, res)
		})
	}
}

func TestBuildTLSConfig(t *testing.T) {
	tests := []struct {
		name           string
		keyFile        string
		certFile       string
		truststoreFile string
		expected       *config.TLS
		expectedErrMsg string
	}{{
		name:     "it should return a nil config if none are set",
		expected: nil,
	}, {
		name:           "it should fail if truststoreFile is empty",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		expectedErrMsg: "--tls-truststore is required to enable TLS",
	}, {
		name:           "it should fail if certFile is present but keyFile is empty",
		certFile:       "cert.pem",
		truststoreFile: "trust.pem",
		expectedErrMsg: "if --tls-cert is passed, then --tls-key must be passed to enable" +
			" TLS authentication",
	}, {
		name:           "it should fail if keyFile is present but certFile is empty",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		expectedErrMsg: "if --tls-key is passed, then --tls-cert must be passed to enable" +
			" TLS authentication",
	}, {
		name:           "it should build the config with only a truststore",
		truststoreFile: "trust.pem",
		expected:       &config.TLS{TruststoreFile: "trust.pem"},
	}, {
		name:           "it should build the config with all fields",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		expected: &config.TLS{
			CertFile:       "cert.pem",
			KeyFile:        "key.pem",
			TruststoreFile: "trust.pem",
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			res, err := common.BuildTLSConfig(
				&tt.certFile,
				&tt.keyFile,
				&tt.truststoreFile,
			)()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
			}
			require.Exactly(st, tt.expected, res)
		})
	}
}
