// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
package cluster

import (
	"bytes"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

func newClientMock() *mocks.MockClient {
	return &mocks.MockClient{
		MockGetOffset: func(string, int32, int64) (int64, error) {
			return 50, nil
		},
	}
}

func newAdminMock() *mocks.MockAdmin {
	return &mocks.MockAdmin{
		MockListConsumerGroups: func() (map[string]string, error) {
			return map[string]string{"group-0": "consumer"}, nil
		},
		MockDescribeConsumerGroups: func(groups []string) (descs []*sarama.GroupDescription, err error) {
			for _, groupId := range groups {
				descs = append(descs,
					&sarama.GroupDescription{
						GroupId: groupId,
					})
			}
			return descs, nil
		},
		MockListConsumerGroupOffsets: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
			r := sarama.OffsetFetchResponse{}
			r.Blocks = map[string]map[int32]*sarama.OffsetFetchResponseBlock{}
			r.Blocks["topic-0"] = map[int32]*sarama.OffsetFetchResponseBlock{}
			r.Blocks["topic-0"][0] = &sarama.OffsetFetchResponseBlock{
				Offset: 100,
			}
			return &r, nil
		},
	}
}

func TestOffsetsCmd(t *testing.T) {
	tests := []struct {
		name           string
		cmd            func(func() (sarama.Client, error), func(sarama.Client) (sarama.ClusterAdmin, error)) *cobra.Command
		args           []string
		expectedOutput string
		expectedErr    string
	}{
		{
			name: "create should output info about the created topic (custom values)",
			cmd:  NewOffsetsCommand,
			expectedOutput: `  GROUP    TOPIC    PARTITION  LAG  LAG %     COMMITTED  LATEST  CONSUMER  CLIENT-HOST  CLIENT-ID  
  group-0  topic-0  0          -50  0.000000  100        50      -                                 
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := func() (sarama.Client, error) {
				return newClientMock(), nil
			}
			admin := func(_ sarama.Client) (sarama.ClusterAdmin, error) {
				return newAdminMock(), nil
			}
			var out bytes.Buffer
			cmd := NewOffsetsCommand(client, admin)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.expectedOutput, out.String())
		})
	}
}
