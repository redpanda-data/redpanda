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
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestStop(t *testing.T) {
	tests := []struct {
		name           string
		client         func() (common.Client, error)
		expectedErrMsg string
		expectedOutput []string
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
			expectedOutput: []string{
				"Stopping node 2",
				"Couldn't stop node 2",
				"Stopping node 1",
				"Couldn't stop node 1",
				"Stopping node 0",
				"Couldn't stop node 0",
				"Don't stop me now",
			},
		},
		{
			name: "it should fail if the containers can't be listed",
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
			expectedErrMsg: "Can't list",
		},
		{
			name: "it should fail if the containers can't be inspected",
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
			expectedOutput: []string{
				"Stopping node 0",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			var out bytes.Buffer
			c, err := tt.client()
			require.NoError(st, err)
			logrus.SetOutput(&out)
			logrus.SetLevel(logrus.DebugLevel)
			err = stopCluster(c)
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
