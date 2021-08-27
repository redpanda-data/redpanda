// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/topic"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

type mockClient struct {
	// add an anonymous interface to trick Go into thinking that this struct
	// implements it 100%.
	sarama.Client
}

func (_ *mockClient) Close() error {
	return nil
}

func generatePartitions(no int) []*sarama.PartitionMetadata {
	ps := []*sarama.PartitionMetadata{}
	for i := 0; i < no; i++ {
		ps = append(
			ps,
			&sarama.PartitionMetadata{
				ID:       int32(i),
				Leader:   1,
				Replicas: []int32{1},
				Isr:      []int32{1},
			},
		)
	}
	return ps
}

func TestTopicCmd(t *testing.T) {
	tests := []struct {
		name           string
		admin          *mocks.MockAdmin
		cmd            func(func() (sarama.ClusterAdmin, error)) *cobra.Command
		args           []string
		expectedOutput string
		expectedErr    string
	}{
		{
			name:           "create should output info about the created topic (custom values)",
			cmd:            topic.NewCreateCommand,
			args:           []string{"Seattle", "--partitions", "2", "--replicas", "3", "--compact"},
			expectedOutput: "Created topic 'Seattle'.\nYou may check its config with\n\nrpk topic describe 'Seattle'\n\n",
		},
		{
			name:           "create should allow passing arbitrary topic config",
			cmd:            topic.NewCreateCommand,
			args:           []string{"San Francisco", "--topic-config", "custom.config:value", "--topic-config", "another.config:anothervalue"},
			expectedOutput: "Created topic 'San Francisco'.\nYou may check its config with\n\nrpk topic describe 'San Francisco'\n\n",
		},
		{
			name:           "create should allow passing comma-separated config values",
			cmd:            topic.NewCreateCommand,
			args:           []string{"San Francisco", "-c", "custom.config:value", "-c", "cleanup.policy:cleanup,compact"},
			expectedOutput: "Created topic 'San Francisco'.\nYou may check its config with\n\nrpk topic describe 'San Francisco'\n\n",
		},
		{
			name:        "create should fail if no topic is passed",
			cmd:         topic.NewCreateCommand,
			args:        []string{},
			expectedErr: "topic's name is missing.",
		},
		{
			name: "create should fail if the topic creation req fails",
			cmd:  topic.NewCreateCommand,
			args: []string{"Chicago"},
			admin: &mocks.MockAdmin{
				MockCreateTopic: func(string, *sarama.TopicDetail, bool) error {
					return errors.New("no bueno error")
				},
			},
			expectedErr: "no bueno error",
		},
		{
			name:           "delete should output the name of the deleted topic",
			cmd:            topic.NewDeleteCommand,
			args:           []string{"Medellin"},
			expectedOutput: "Deleted topic 'Medellin'.",
		},
		{
			name: "delete should fail if the topic deletion req fails",
			cmd:  topic.NewDeleteCommand,
			args: []string{"Leticia"},
			admin: &mocks.MockAdmin{
				MockDeleteTopic: func(string) error {
					return errors.New("that topic don't exist, yo")
				},
			},
			expectedErr: "that topic don't exist, yo",
		},
		{
			name:        "delete should fail if no topic is passed",
			cmd:         topic.NewDeleteCommand,
			args:        []string{},
			expectedErr: "topic's name is missing.",
		},
		{
			name:           "set-config should output the given config key-value pair",
			cmd:            topic.NewSetConfigCommand,
			args:           []string{"Panama", "somekey=somevalue", "someotherkey=someothervalue"},
			expectedOutput: "Added configs somekey=somevalue, someotherkey=someothervalue to topic 'Panama'.",
		},
		{
			name:           "set-config should allow passing negative numbers and not parse them as flags",
			cmd:            topic.NewSetConfigCommand,
			args:           []string{"Panama", "retention.ms=-1"},
			expectedOutput: "Added configs retention.ms=-1 to topic 'Panama'.",
		},
		{
			name: "set-config should fail if the req fails",
			cmd:  topic.NewSetConfigCommand,
			args: []string{"Chiriqui", "k=v"},
			admin: &mocks.MockAdmin{
				MockAlterConfig: func(
					sarama.ConfigResourceType,
					string,
					map[string]*string,
					bool,
				) error {
					return errors.New("can't set the config for some reason")
				},
			},
			expectedErr: "can't set the config for some reason",
		},
		{
			name:        "set-config should fail if no topic is passed",
			cmd:         topic.NewSetConfigCommand,
			args:        []string{},
			expectedErr: "a topic name and at least one key=value pair is required",
		},
		{
			name:        "set-config should fail if no key-value pairs are passed",
			cmd:         topic.NewSetConfigCommand,
			args:        []string{"Chepo"},
			expectedErr: "a topic name and at least one key=value pair is required",
		},
		{
			name:        "set-config should fail if a key-value pair is invalid",
			cmd:         topic.NewSetConfigCommand,
			args:        []string{"Chepo", "key=value", "keyequalsvalue"},
			expectedErr: "invalid element 'keyequalsvalue'. Expected format <key>=<value>",
		},
		{
			name: "list should output the list of topics",
			cmd:  topic.NewListCommand,
			admin: &mocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					return map[string]sarama.TopicDetail{
						"tokyo": {
							NumPartitions:     2,
							ReplicationFactor: 3,
						},
						"kyoto": {
							NumPartitions:     10,
							ReplicationFactor: 2,
						},
						"fukushima": {
							NumPartitions:     7,
							ReplicationFactor: 3,
						},
					}, nil
				},
			},
			args: []string{},
			expectedOutput: `  Name       Partitions  Replicas  
  fukushima  7           3         
  kyoto      10          2         
  tokyo      2           3         
`,
		},
		{
			name: "list should fail if the req fails",
			cmd:  topic.NewListCommand,
			args: []string{},
			admin: &mocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					return nil, errors.New("an error happened :(")
				},
			},
			expectedErr: "an error happened :(",
		},
		{
			name: "list should output a message if there are no topics",
			cmd:  topic.NewListCommand,
			admin: &mocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					return map[string]sarama.TopicDetail{}, nil
				},
			},
			expectedOutput: "No topics found.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admin := func() (sarama.ClusterAdmin, error) {
				if tt.admin != nil {
					return tt.admin, nil
				}
				return &mocks.MockAdmin{}, nil
			}
			var out bytes.Buffer
			cmd := tt.cmd(admin)
			cmd.SetArgs(tt.args)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Contains(t, out.String(), tt.expectedOutput)
		})
	}
}
