package container

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
	"vectorized/pkg/cli/cmd/container/common"

	"github.com/docker/docker/api/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestPurge(t *testing.T) {
	tests := []struct {
		name           string
		client         func() (common.Client, error)
		expectedErrMsg string
		expectedOutput []string
		before         func(afero.Fs) error
		check          func(afero.Fs, *testing.T)
	}{
		{
			name: "it should log if the containers can't be stopped",
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockContainerStop: func(
						_ context.Context,
						_ string,
						_ *time.Duration,
					) error {
						return errors.New("Don't stop me now")
					},
				}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			expectedOutput: []string{
				"Stopping node 0",
				"Couldn't stop node 0",
				"Don't stop me now",
			},
		},
		{
			name: "it should do nothing if there's no cluster",
			client: func() (common.Client, error) {
				return &common.MockClient{}, nil
			},
			expectedOutput: []string{
				`No nodes to remove.\nYou may start a new local cluster with 'rpk container start'`,
			},
		},
		{
			name: "it should stop the current cluster",
			client: func() (common.Client, error) {
				return &common.MockClient{}, nil
			},
			before: func(fs afero.Fs) error {
				err := fs.MkdirAll(common.ConfDir(0), 0755)
				if err != nil {
					return err
				}
				err = fs.MkdirAll(common.ConfDir(1), 0755)
				if err != nil {
					return err
				}
				return fs.MkdirAll(common.ConfDir(2), 0755)
			},
			expectedOutput: []string{
				"Stopped current cluster.",
				"Deleted data for node 0",
				"Removed container 'rp-node-0'",
				"Deleted data for node 1",
				"Removed container 'rp-node-1'",
				"Deleted data for node 2",
				"Removed container 'rp-node-2'",
			},
		},
		{
			name: "it should fail if it fails to remove a container",
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockContainerRemove: func(
						context.Context,
						string,
						types.ContainerRemoveOptions,
					) error {
						return errors.New("Not going anywhere!")
					},
				}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			expectedErrMsg: "Not going anywhere!",
		},
		{
			name: "it should fail if it fails to delete the network",
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockNetworkRemove: func(
						context.Context,
						string,
					) error {
						return errors.New("Can't delete network")
					},
				}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			expectedErrMsg: "Can't delete network",
		},
		{
			name: "it should succeed if the network has been removed",
			client: func() (common.Client, error) {
				return &common.MockClient{
					MockNetworkRemove: func(
						context.Context,
						string,
					) error {
						return errors.New("Network not found")
					},
					MockIsErrNotFound: func(_ error) bool {
						return true
					},
				}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			expectedOutput: []string{"Deleted cluster data."},
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
			logrus.SetLevel(logrus.DebugLevel)
			err = purgeCluster(fs, c)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)

			if len(tt.expectedOutput) != 0 {
				for _, o := range tt.expectedOutput {
					require.Contains(
						st,
						out.String(),
						o,
					)
				}
			}
			if tt.check != nil {
				tt.check(fs, st)
			}
		})
	}
}
