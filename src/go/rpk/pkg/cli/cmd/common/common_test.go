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
	"os"
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
		before   func()
		cleanup  func()
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
		name: "it should take the value from the env vars if the flag wasn't passed",
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
		before: func() {
			os.Setenv("REDPANDA_BROKERS", "192.168.34.12:9093,123.4.5.78:9092")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_BROKERS")
		},
		expected: []string{"192.168.34.12:9093", "123.4.5.78:9092"},
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
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
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
		before         func()
		cleanup        func()
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
	}, {
		name: "it should pick up the values from env vars if the vars' values is empty",
		before: func() {
			os.Setenv("REDPANDA_SASL_USERNAME", "ringo")
			os.Setenv("REDPANDA_SASL_PASSWORD", "octopussgarden66")
			os.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_USERNAME")
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_MECHANISM")
		},
		expected: &config.SCRAM{User: "ringo", Password: "octopussgarden66", Type: "SCRAM-SHA-512"},
	}, {
		name:      "it should give priority to values set through the flags",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-512",
		before: func() {
			os.Setenv("REDPANDA_SASL_USERNAME", "ringo")
			os.Setenv("REDPANDA_SASL_PASSWORD", "octopussgarden66")
			os.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_USERNAME")
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_MECHANISM")
		},
		// Disregards the env vars' values
		expected: &config.SCRAM{User: "usuario", Password: "contraseño", Type: "SCRAM-SHA-512"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
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
		before         func()
		cleanup        func()
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
	}, {
		name: "it should pick up the values from env vars if the vars' values is empty",
		before: func() {
			os.Setenv("REDPANDA_TLS_CERT", "./node.crt")
			os.Setenv("REDPANDA_TLS_KEY", "./node.key")
			os.Setenv("REDPANDA_TLS_TRUSTSTORE", "./ca.crt")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_TLS_CERT")
			os.Unsetenv("REDPANDA_TLS_KEY")
			os.Unsetenv("REDPANDA_TLS_TRUSTSTORE")
		},
		expected: &config.TLS{
			CertFile:       "./node.crt",
			KeyFile:        "./node.key",
			TruststoreFile: "./ca.crt",
		},
	}, {
		name:           "it should give priority to values set through the flags",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		before: func() {
			os.Setenv("REDPANDA_TLS_CERT", "./node.crt")
			os.Setenv("REDPANDA_TLS_KEY", "./node.key")
			os.Setenv("REDPANDA_TLS_TRUSTSTORE", "./ca.crt")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_TLS_CERT")
			os.Unsetenv("REDPANDA_TLS_KEY")
			os.Unsetenv("REDPANDA_TLS_TRUSTSTORE")
		},
		// Disregards the env vars' values
		expected: &config.TLS{
			CertFile:       "cert.pem",
			KeyFile:        "key.pem",
			TruststoreFile: "trust.pem",
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
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

func TestCreateAdmin(t *testing.T) {
	tests := []struct {
		name           string
		brokers        func() []string
		configuration  func() (*config.Config, error)
		tlsConfig      func() (*config.TLS, error)
		authConfig     func() (*config.SCRAM, error)
		expectedErrMsg string
	}{{
		name: "the returned closure should fail if configuration fails",
		configuration: func() (*config.Config, error) {
			return nil, errors.New("No config found here")
		},
		expectedErrMsg: "No config found here",
	}, {
		name: "the returned closure should fail if tlsConfig fails",
		tlsConfig: func() (*config.TLS, error) {
			return nil, errors.New("bad tls conf")
		},
		expectedErrMsg: "bad tls conf",
	}, {
		name: "the returned closure should fail if authConfig returns an error other than ErrNoCredentials",
		authConfig: func() (*config.SCRAM, error) {
			return nil, errors.New("Some bad error")
		},
		expectedErrMsg: "Some bad error",
	}, {
		name: "the returned closure shouldn't fail due to authConfig returning ErrNoCredentials",
		authConfig: func() (*config.SCRAM, error) {
			return nil, common.ErrNoCredentials
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			brokers := func() []string {
				return []string{}
			}
			if tt.brokers != nil {
				brokers = tt.brokers
			}

			configuration := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.configuration != nil {
				configuration = tt.configuration
			}

			tlsConfig := func() (*config.TLS, error) {
				return nil, nil
			}
			if tt.tlsConfig != nil {
				tlsConfig = tt.tlsConfig
			}

			authConfig := func() (*config.SCRAM, error) {
				return nil, nil
			}
			if tt.authConfig != nil {
				authConfig = tt.authConfig
			}

			fn := common.CreateAdmin(brokers, configuration, tlsConfig, authConfig)
			_, err := fn()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			// The admin always fails to initialize because the brokers closure
			// always returns an empty slice.
			require.EqualError(st, err, "couldn't connect to redpanda at . Try using --brokers to specify other brokers to connect to.")
		})
	}
}
func TestCreateClient(t *testing.T) {
	tests := []struct {
		name           string
		brokers        func() []string
		configuration  func() (*config.Config, error)
		tlsConfig      func() (*config.TLS, error)
		authConfig     func() (*config.SCRAM, error)
		expectedErrMsg string
	}{{
		name: "the returned closure should fail if configuration fails",
		configuration: func() (*config.Config, error) {
			return nil, errors.New("No config found here")
		},
		expectedErrMsg: "No config found here",
	}, {
		name: "the returned closure should fail if tlsConfig fails",
		tlsConfig: func() (*config.TLS, error) {
			return nil, errors.New("bad tls conf")
		},
		expectedErrMsg: "bad tls conf",
	}, {
		name: "the returned closure should fail if authConfig returns an error other than ErrNoCredentials",
		authConfig: func() (*config.SCRAM, error) {
			return nil, errors.New("Some bad error")
		},
		expectedErrMsg: "Some bad error",
	}, {
		name: "the returned closure shouldn't fail due to authConfig returning ErrNoCredentials",
		authConfig: func() (*config.SCRAM, error) {
			return nil, common.ErrNoCredentials
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			brokers := func() []string {
				return []string{}
			}
			if tt.brokers != nil {
				brokers = tt.brokers
			}

			configuration := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.configuration != nil {
				configuration = tt.configuration
			}

			tlsConfig := func() (*config.TLS, error) {
				return nil, nil
			}
			if tt.tlsConfig != nil {
				tlsConfig = tt.tlsConfig
			}

			authConfig := func() (*config.SCRAM, error) {
				return nil, nil
			}
			if tt.authConfig != nil {
				authConfig = tt.authConfig
			}

			fn := common.CreateClient(brokers, configuration, tlsConfig, authConfig)
			_, err := fn()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			// The client always fails to initialize because the brokers closure
			// always returns an empty slice.
			require.EqualError(st, err, "couldn't connect to redpanda at . Try using --brokers to specify other brokers to connect to.")
		})
	}
}
