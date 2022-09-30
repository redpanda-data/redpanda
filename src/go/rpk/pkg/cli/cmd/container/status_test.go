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
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestStatus(t *testing.T) {
	tests := []struct {
		name           string
		client         func() (common.Client, error)
		expectedErrMsg string
		expectedOutput []string
	}{
		{
			name: "it should state if no containers exist",
			client: func() (common.Client, error) {
				return &common.MockClient{}, nil
			},
			expectedOutput: []string{
				"No Redpanda nodes detected - use `rpk container start` or check `docker ps` if you expected nodes",
			},
		},
		{
			name: "it should show container info",
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
						}, nil
					},
				}, nil
			},
			expectedOutput: []string{
				`
  0        Up, I guess?  127.0.0.1:89080  -              -              
  1        Up, I guess?  127.0.0.1:89080  -              -              `,
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
			_, err = renderClusterInfo(c)
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
