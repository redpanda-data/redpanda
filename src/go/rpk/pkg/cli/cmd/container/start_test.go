// Copyright 2020 Vectorized, Inc.
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
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
)

func noopCheck(_ []node) func() error {
	return func() error {
		return nil
	}
}

func TestStart(t *testing.T) {
	tests := []struct {
		name		string
		client		func() (common.Client, error)
		nodes		uint
		check		func([]node) func() error
		expectedErrMsg	string
		expectedOutput	string
	}{
		{
			name:	"it should fail if the img can't be pulled and imgs can't be listed",
			client: func() (common.Client, error) {
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
				" wasn't found either.",
		},
		{
			name:	"it should fail if the img couldn't be pulled bc of internet conn issues",
			client: func() (common.Client, error) {
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
			name:	"it should fail if the img can't be pulled and it isn't avail. locally",
			client: func() (common.Client, error) {
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
				" wasn't found either.",
		},
		{
			name:	"it should fail if creating the network fails",
			client: func() (common.Client, error) {
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
			expectedErrMsg:	"Network create go boom",
		},
		{
			name:	"it should fail if inspecting the network fails",
			client: func() (common.Client, error) {
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
			expectedErrMsg:	"Can't inspect the network",
		},
		{
			name:	"it should fail if the network config is corrupted",
			client: func() (common.Client, error) {
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
							Name:	"rpnet",
							IPAM:	ipam,
						}
						return res, nil
					},
				}, nil
			},
			expectedErrMsg:	"'rpnet' network config is corrupted",
		},
		{
			name:	"it should fail if listing the containers fails",
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockContainerList: func(
						_ context.Context,
						_ types.ContainerListOptions,
					) ([]types.Container, error) {
						return nil, errors.New("Can't list")
					},
				}, nil
			},
			expectedErrMsg:	"Can't list",
		},
		{
			name:	"it should fail if inspecting existing containers fails",
			client: func() (common.Client, error) {
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
								ID:	"a",
								Labels: map[string]string{
									"node-id": "0",
								},
							},
						}, nil
					},
				}, nil
			},
			expectedErrMsg:	"Can't inspect",
		},
		{
			name:	"it should fail if creating the container fails",
			client: func() (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:		"172.24.1.0/24",
							Gateway:	"172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name:	"rpnet",
							IPAM:	ipam,
						}
						return res, nil
					},
					// ContainerCreate fails.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{}
						return body, errors.New(
							"Can't create container",
						)
					},
				}, nil
			},
			expectedErrMsg:	"Can't create container",
		},
		{
			name:	"it should allow creating a single container",
			nodes:	1,
			client: func() (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:		"172.24.1.0/24",
							Gateway:	"172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name:	"rpnet",
							IPAM:	ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			expectedOutput:	`Cluster started! You may use 'rpk api' to interact with the cluster. E.g:\n\nrpk api status`,
		},
		{
			name:	"it should allow creating multiple containers",
			nodes:	3,
			client: func() (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:		"172.24.1.0/24",
							Gateway:	"172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name:	"rpnet",
							IPAM:	ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			expectedOutput:	`Cluster started! You may use 'rpk api' to interact with the cluster. E.g:\n\nrpk api status`,
		},
		{
			name:	"it should do nothing if there's an existing running cluster",
			nodes:	1,
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockContainerInspect:	common.MockContainerInspect,
					MockContainerList: func(
						_ context.Context,
						_ types.ContainerListOptions,
					) ([]types.Container, error) {
						return []types.Container{
							{
								ID:	"a",
								Labels: map[string]string{
									"node-id": "0",
								},
							},
							{
								ID:	"b",
								Labels: map[string]string{
									"node-id": "1",
								},
							},
							{
								ID:	"c",
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
			name:	"it should fail if the cluster doesn't form",
			nodes:	3,
			client: func() (common.Client, error) {
				return &common.MockClient{
					// NetworkInspect succeeds returning the
					// expected config.
					MockNetworkInspect: func(
						_ context.Context,
						_ string,
						_ types.NetworkInspectOptions,
					) (types.NetworkResource, error) {
						ipamConf := network.IPAMConfig{
							Subnet:		"172.24.1.0/24",
							Gateway:	"172.24.1.1",
						}
						ipam := network.IPAM{
							Config: []network.IPAMConfig{
								ipamConf,
							},
						}
						res := types.NetworkResource{
							Name:	"rpnet",
							IPAM:	ipam,
						}
						return res, nil
					},
					// ContainerCreate succeeds.
					MockContainerCreate: func(
						_ context.Context,
						_ *container.Config,
						_ *container.HostConfig,
						_ *network.NetworkingConfig,
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
			expectedErrMsg:	`Some weird error`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			var out bytes.Buffer
			c, err := tt.client()
			require.NoError(st, err)
			logrus.SetOutput(&out)
			check := noopCheck
			if tt.check != nil {
				check = tt.check
			}
			retries := uint(10)
			err = startCluster(c, tt.nodes, check, retries)
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
