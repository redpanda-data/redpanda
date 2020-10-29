package container

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"vectorized/pkg/cli/cmd/container/common"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestStart(t *testing.T) {
	tests := []struct {
		name           string
		client         func() (common.Client, error)
		nodes          uint
		expectedErrMsg string
		expectedOutput string
		before         func(afero.Fs) error
		check          func(afero.Fs, *testing.T)
	}{
		{
			name: "it should fail if the img can't be pulled and imgs can't be listed",
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
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, false, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name: "it should fail if the img couldn't be pulled bc of internet conn issues",
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
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, false, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name: "it should fail if the img can't be pulled and it isn't avail. locally",
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
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, false, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name: "it should fail if creating the network fails",
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
			expectedErrMsg: "Network create go boom",
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, true, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
				ok, err = common.CheckFiles(fs, st, false, common.NodeDir(0))
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name: "it should fail if inspecting the network fails",
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
			expectedErrMsg: "Can't inspect the network",
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, true, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
				ok, err = common.CheckFiles(fs, st, false, common.NodeDir(0))
				require.NoError(st, err)
				require.True(st, ok)

			},
		},
		{
			name: "it should fail if the network config is corrupted",
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
							Name: "rpnet",
							IPAM: ipam,
						}
						return res, nil
					},
				}, nil
			},
			expectedErrMsg: "'rpnet' network config is corrupted",
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, true, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
				ok, err = common.CheckFiles(fs, st, false, common.NodeDir(0))
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name: "it should fail if creating the container fails",
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
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(fs, st, true, common.ClusterDir())
				require.NoError(st, err)
				require.True(st, ok)
				ok, err = common.CheckFiles(fs, st, false, common.NodeDir(0))
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name:  "it should allow creating a single container",
			nodes: 1,
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
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			expectedOutput: `Cluster started! You may use 'rpk api' to interact with the cluster. E.g:\n\nrpk api status --brokers 172.24.1.2:9092`,
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(
					fs,
					st,
					true,
					common.DataDir(0),
					common.ConfPath(0),
				)
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name:  "it should allow creating multiple containers",
			nodes: 3,
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
						_ string,
					) (container.ContainerCreateCreatedBody, error) {
						body := container.ContainerCreateCreatedBody{
							ID: "container-1",
						}
						return body, nil
					},
				}, nil
			},
			expectedOutput: `Cluster started! You may use 'rpk api' to interact with the cluster. E.g:\n\nrpk api status --brokers 172.24.1.2:9092`,
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(
					fs,
					st,
					true,
					common.DataDir(0),
					common.ConfPath(0),
					common.DataDir(1),
					common.ConfPath(1),
					common.DataDir(2),
					common.ConfPath(2),
				)
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name:  "it should do nothing if there's an existing running cluster",
			nodes: 1,
			client: func() (common.Client, error) {
				return &common.MockClient{}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(
					fs,
					st,
					true,
					common.ConfDir(0),
				)
				require.NoError(st, err)
				require.True(st, ok)
			},
		},
		{
			name:  "it should fail if there's an existing cluster but the containers were removed",
			nodes: 1,
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockContainerInspect: func(
						_ context.Context,
						_ string,
					) (types.ContainerJSON, error) {
						return types.ContainerJSON{},
							errors.New("Image not found")
					},
					MockIsErrNotFound: func(_ error) bool {
						return true
					},
				}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			check: func(fs afero.Fs, st *testing.T) {
				ok, err := common.CheckFiles(
					fs,
					st,
					true,
					common.ConfDir(0),
				)
				require.NoError(st, err)
				require.True(st, ok)
			},
			expectedErrMsg: `Found data for an existing cluster, but the container for node 0 was removed.
Please run 'rpk container purge' to delete all remaining data and create a new cluster with 'rpk container start'.`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			var out bytes.Buffer
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				require.NoError(st, tt.before(fs))
			}
			c, err := tt.client()
			require.NoError(st, err)
			logrus.SetOutput(&out)
			err = startCluster(fs, c, tt.nodes)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
			} else {
				require.NoError(st, err)

				if tt.expectedOutput != "" {
					require.Contains(st, out.String(), tt.expectedOutput)
				}
			}
			if tt.check != nil {
				tt.check(fs, st)
			}
		})
	}
}
