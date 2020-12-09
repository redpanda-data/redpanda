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
	"testing"
	"time"
	"vectorized/pkg/cli/cmd/container/common"

	"github.com/docker/docker/api/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestStop(t *testing.T) {

	tests := []struct {
		name           string
		client         func() (common.Client, error)
		expectedErrMsg string
		expectedOutput []string
		before         func(afero.Fs) error
	}{
		{
			name: "it should log if the container can't be stopped",
			client: func() (common.Client, error) {
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
				`No cluster available.\nYou may start a new cluster with 'rpk container start'`,
			},
		},
		{
			name: "it should stop the current cluster",
			client: func() (common.Client, error) {
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
						}, nil
					},
				}, nil
			},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll(common.ConfDir(0), 0755)
			},
			expectedOutput: []string{
				"Stopping node 0",
			},
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
			err = stopCluster(fs, c)
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

		})
	}
}
