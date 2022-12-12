// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package container

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noopCheck(_ []node) func() error {
	return func() error {
		return nil
	}
}

func TestStart(t *testing.T) {
	happyPathClusterPorts := clusterPorts{
		kafka: []uint{9092},
	}
	tests := []struct {
		name           string
		client         func(st *testing.T) (common.Client, error)
		ports          clusterPorts
		nodes          uint
		check          func([]node) func() error
		expectedErrMsg string
		expectedOutput string
	}{
		{
			name:  "fail if the img can't be pulled and imgs can't be listed",
			ports: happyPathClusterPorts,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockImagePull: func(
						_ context.Context,
						_ string,
						_ types.ImagePullOptions,
					) (io.ReadCloser, error) {
						return nil, errors.New("Can't pull")
					},
					MockImageList: func(
						_ context.Context,
						_ types.ImageListOptions,
					) ([]types.ImageSummary, error) {
						return nil, errors.New("Can't list")
					},
				}, nil
			},
			expectedErrMsg: "Couldn't pull image and a local one" +
				" wasn't found either: Can't pull",
		},
		{
			name:  "fail if the img couldn't be pulled bc of internet conn issues",
			ports: happyPathClusterPorts,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockImagePull: func(
						_ context.Context,
						_ string,
						_ types.ImagePullOptions,
					) (io.ReadCloser, error) {
						return nil, errors.New("Can't pull")
					},
					MockImageList: func(
						_ context.Context,
						_ types.ImageListOptions,
					) ([]types.ImageSummary, error) {
						return nil, errors.New("Can't list")
					},
					MockIsErrConnectionFailed: func(_ error) bool {
						return true
					},
				}, nil
			},
			expectedErrMsg: `Couldn't pull image and a local one wasn't found either.
Please check your internet connection and try again.`,
		},
		{
			name:  "fail if the img can't be pulled and it isn't avail. locally",
			ports: happyPathClusterPorts,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockImagePull: func(
						_ context.Context,
						_ string,
						_ types.ImagePullOptions,
					) (io.ReadCloser, error) {
						return nil, errors.New("Can't pull")
					},
					MockImageList: func(
						_ context.Context,
						_ types.ImageListOptions,
					) ([]types.ImageSummary, error) {
						return []types.ImageSummary{}, nil
					},
				}, nil
			},
			expectedErrMsg: "Couldn't pull image and a local one" +
				" wasn't found either: Can't pull",
		},
		{
			name:  "fail if creating the network fails",
			ports: happyPathClusterPorts,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockNetworkCreate: func(
						_ context.Context,
						_ string,
						_ types.NetworkCreate,
					) (types.NetworkCreateResponse, error) {
						res := types.NetworkCreateResponse{}
						return res, errors.New(
							"Network create go boom",
						)
					},
				}, nil
			},
			expectedErrMsg: "Network create go boom",
		},
		{
			name:  "fail if inspecting the network fails",
			ports: happyPathClusterPorts,
			nodes: 1,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						res := types.NetworkResource{}
						return res, errors.New(
							"Can't inspect the network",
						)
					},
				}, nil
			},
			expectedErrMsg: "Can't inspect the network",
		},
		{
			name:  "fail if the network config is corrupted",
			ports: happyPathClusterPorts,
			nodes: 1,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipam := network.IPAM{
							Config: []network.IPAMConfig{},
						}
						res := types.NetworkResource{
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
				}, nil
			},
			expectedErrMsg: "'rpnet' network config is corrupted",
		},
		{
			name:  "fail if listing the containers fails",
			ports: happyPathClusterPorts,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockContainerList: func(
						_ context.Context,
						_ types.ContainerListOptions,
					) ([]types.Container, error) {
						return nil, errors.New("Can't list")
					},
				}, nil
			},
			expectedErrMsg: "Can't list",
		},
		{
			name:  "fail if inspecting existing containers fails",
			ports: happyPathClusterPorts,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockContainerInspect: func(
						_ context.Context,
						_ string,
					) (types.ContainerJSON, error) {
						return types.ContainerJSON{},
							errors.New("Can't inspect")
					},
					MockContainerList: func(
						_ context.Context,
						_ types.ContainerListOptions,
					) ([]types.Container, error) {
						return []types.Container{
							{
								ID: "a",
								Labels: map[string]string{
									"node-id": "0",
								},
							},
						}, nil
					},
				}, nil
			},
			expectedErrMsg: "Can't inspect",
		},
		{
			name:  "fail if creating the container fails",
			ports: happyPathClusterPorts,
			nodes: 1,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:  "172.24.1.0/24",
							Gateway: "172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
					// ContainerCreate fails.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ *specs.Platform,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{}
						return body, errors.New(
							"Can't create container",
						)
					},
				}, nil
			},
			expectedErrMsg: "Can't create container",
		},
		{
			name:  "allow creating a single container",
			ports: happyPathClusterPorts,
			nodes: 1,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:  "172.24.1.0/24",
							Gateway: "172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ *specs.Platform,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			expectedOutput: `Cluster started! You may use rpk to interact with it.`,
		},
		{
			name: "allow creating multiple containers",
			ports: clusterPorts{
				kafka: []uint{9092, 9093, 9094},
			},
			nodes: 3,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:  "172.24.1.0/24",
							Gateway: "172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ *specs.Platform,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			expectedOutput: `Cluster started! You may use rpk to interact with it.`,
		},
		{
			name:  "do nothing if there's an existing running cluster",
			ports: happyPathClusterPorts,
			nodes: 1,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					MockContainerInspect: common.MockContainerInspect,
					MockContainerList: func(
						_ context.Context,
						_ types.ContainerListOptions,
					) ([]types.Container, error) {
						return []types.Container{
							{
								ID: "a",
								Labels: map[string]string{
									"node-id": "0",
								},
							},
							{
								ID: "b",
								Labels: map[string]string{
									"node-id": "1",
								},
							},
							{
								ID: "c",
								Labels: map[string]string{
									"node-id": "2",
								},
							},
						}, nil
					},
				}, nil
			},
		},
		{
			name: "fail if the cluster doesn't form",
			ports: clusterPorts{
				kafka: []uint{9092, 9093, 9094},
			},
			nodes: 3,
			client: func(_ *testing.T) (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:  "172.24.1.0/24",
							Gateway: "172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ *specs.Platform,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			check: func(_ []node) func() error {
				return func() error {
					return errors.New("Some weird error")
				}
			},
			expectedErrMsg: `Some weird error`,
		},
		{
			name: "pass the right flags",
			ports: clusterPorts{
				kafka: []uint{9092, 9093, 9094},
			},
			nodes: 3,
			client: func(st *testing.T) (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:  "172.24.1.0/24",
							Gateway: "172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						cc *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ *specs.Platform,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						// If the node is not the seed, check
						// that --seed is passed and that
						// the format is right
						if cc.Hostname != "rp-node-0" {
							require.Contains(
								st,
								strings.Join(cc.Cmd, " "),
								"--seeds 172.24.1.2:33145",
							)
						}
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			check: func(_ []node) func() error {
				return func() error {
					return errors.New("Some weird error")
				}
			},
			expectedErrMsg: `Some weird error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			var out bytes.Buffer
			c, err := tt.client(st)
			require.NoError(st, err)
			logrus.SetOutput(&out)
			check := noopCheck
			if tt.check != nil {
				check = tt.check
			}
			retries := uint(10)
			err = startCluster(
				c,
				tt.nodes,
				tt.ports,
				check,
				retries,
				common.DefaultImage(),
				nil,
			)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
			} else {
				require.NoError(st, err)

				if tt.expectedOutput != "" {
					require.Contains(st, out.String(), tt.expectedOutput)
				}
			}
		})
	}
}

func TestCollectFlags(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{{
		name: "it should collect the --set flags and values",
		input: []string{
			"arg1",
			"--flag",
			"--set", "k1=v1",
			"--other-flag", "value",
			"--set", `path.to.field={"field":"this is a json obj","arr":[]}`,
		},
		expected: []string{
			"--set", "k1=v1",
			"--set", `path.to.field={"field":"this is a json obj","arr":[]}`,
		},
	}, {
		name: "it should return an empty list if there are no values",
		input: []string{
			"arg1",
			"--flag",
			"--other-flag", "value",
		},
		expected: []string{},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			res := collectFlags(tt.input, "--set")
			require.Exactly(st, tt.expected, res)
		})
	}
}

func TestAssignPorts(t *testing.T) {
	tests := []struct {
		name       string
		ports      []string
		takenPorts map[uint]struct{}
		nodes      uint
		expErr     string
		exp        []uint
		isAny      bool
	}{
		{
			name:       "not allow if ports is greater than the number of nodes",
			ports:      []string{"9092", "9093"},
			takenPorts: map[uint]struct{}{},
			nodes:      1,
			expErr:     "--test-port should not exceed the total number of nodes (1)",
			exp:        []uint(nil),
		},
		{
			name:       "return 0 nodes if there are no ports passed",
			ports:      []string{},
			takenPorts: map[uint]struct{}{},
			nodes:      1,
			expErr:     "",
			exp:        []uint(nil),
		},
		{
			name:       "validate none uints",
			ports:      []string{"this_is_not_none"},
			takenPorts: map[uint]struct{}{},
			nodes:      1,
			expErr:     "provided port \"this_is_not_none\" is not a valid uint nor a valid string ('any', 'none', 'skip')",
			exp:        []uint(nil),
		},
		{
			name:       "validate ports that is not within the port range",
			ports:      []string{"65536"},
			takenPorts: map[uint]struct{}{},
			nodes:      1,
			expErr:     "provided port \"65536\" exceeded the range of transport layer range of 1025 - 65535",
			exp:        []uint(nil),
		},
		{
			name:       "don't allow 'none' and 'any'",
			ports:      []string{"none", "any"},
			takenPorts: map[uint]struct{}{},
			nodes:      2,
			expErr:     "cannot specify 'any' and ('none' or 'skip') together",
			exp:        []uint(nil),
		},
		{
			name:       "don't allow 'none' or 'any' specified with ports",
			ports:      []string{"none", "9092"},
			takenPorts: map[uint]struct{}{},
			nodes:      2,
			expErr:     "cannot specify 'any', 'none', or 'skip' with specific ports",
			exp:        []uint(nil),
		},
		{
			name:       "would return a single element array with the element 0 to signal that this is for random assignment",
			ports:      []string{"any"},
			takenPorts: map[uint]struct{}{},
			isAny:      true,
			nodes:      5,
			exp:        []uint{0},
			expErr:     "",
		},
		{
			name:       "use the highest port as a base port",
			ports:      []string{"9092", "9082"},
			takenPorts: map[uint]struct{}{},
			nodes:      3,
			expErr:     "",
			exp:        []uint{9082, 9092, 9093},
		},
		{
			name:       "don't allow overlaps",
			ports:      []string{"9092"},
			takenPorts: map[uint]struct{}{9092: {}},
			nodes:      3,
			expErr:     "--test-port port (9092) has an overlap with other flags",
		},
		{
			name:       "return no ports when none is specified",
			ports:      []string{"none"},
			takenPorts: map[uint]struct{}{},
			nodes:      3,
			expErr:     "",
			exp:        []uint(nil),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(st *testing.T) {
			assert := assert.New(st)
			assignedPorts, err := assignPorts("test-port", test.ports, test.takenPorts, test.nodes, false)
			if test.expErr != "" {
				assert.Error(err)
				assert.Equal(test.expErr, err.Error())
				return
			} else {
				assert.NoError(err)
				assert.Equal(len(test.exp), len(assignedPorts))
				if len(test.exp) > 0 {
					for i, ep := range test.exp {
						assert.Equal(ep, assignedPorts[i])
					}
				}
			}
		})
	}
}
