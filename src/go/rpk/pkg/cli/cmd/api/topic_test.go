package api

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type mockAdmin struct {
	// add an anonymous interface to trick Go into thinking that this struct
	// implements it 100%.
	sarama.ClusterAdmin
	// add the specific funcs we'll need
	createTopic func(string, *sarama.TopicDetail, bool) error
	deleteTopic func(string) error
}

func (a *mockAdmin) CreateTopic(
	topic string, detail *sarama.TopicDetail, validateOnly bool,
) error {
	if a.createTopic != nil {
		return a.createTopic(topic, detail, validateOnly)
	}
	return nil
}

func (a *mockAdmin) DeleteTopic(topic string) error {
	if a.deleteTopic != nil {
		return a.deleteTopic(topic)
	}
	return nil
}

func (_ *mockAdmin) Close() error {
	return nil
}

func TestTopicCmd(t *testing.T) {
	tests := []struct {
		name           string
		admin          *mockAdmin
		cmd            func(*sarama.ClusterAdmin) *cobra.Command
		args           []string
		expectedOutput string
		expectedErr    string
	}{
		{
			name:           "create should output info about the created topic (default values)",
			cmd:            createTopic,
			args:           []string{"San Francisco"},
			expectedOutput: "Created topic 'San Francisco'. Partitions: 1, replicas: 1, cleanup policy: 'delete'",
		},
		{
			name:           "create should output info about the created topic (custom values)",
			cmd:            createTopic,
			args:           []string{"Seattle", "--partitions", "2", "--replicas", "3", "--compact"},
			expectedOutput: "Created topic 'Seattle'. Partitions: 2, replicas: 3, cleanup policy: 'compact'",
		},
		{
			name:        "create should fail if no topic is passed",
			cmd:         createTopic,
			args:        []string{},
			expectedErr: "accepts 1 arg(s), received 0",
		},
		{
			name: "create should fail if the topic creation req fails",
			cmd:  createTopic,
			args: []string{"Chicago"},
			admin: &mockAdmin{
				createTopic: func(string, *sarama.TopicDetail, bool) error {
					return errors.New("no bueno error")
				},
			},
			expectedErr: "no bueno error",
		},
		{
			name:           "delete should output the name of the deleted topic",
			cmd:            deleteTopic,
			args:           []string{"Medellin"},
			expectedOutput: "Deleted topic 'Medellin'.",
		},
		{
			name: "delete should fail if the topic deletion req fails",
			cmd:  deleteTopic,
			args: []string{"Leticia"},
			admin: &mockAdmin{
				deleteTopic: func(string) error {
					return errors.New("that topic don't exist, yo")
				},
			},
			expectedErr: "that topic don't exist, yo",
		},
		{
			name:        "delete should fail if no topic is passed",
			cmd:         deleteTopic,
			args:        []string{},
			expectedErr: "accepts 1 arg(s), received 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var admin sarama.ClusterAdmin = &mockAdmin{}
			if tt.admin != nil {
				admin = tt.admin
			}
			var out bytes.Buffer
			cmd := tt.cmd(&admin)
			cmd.SetArgs(tt.args)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			if tt.expectedOutput != "" {
				require.Contains(t, out.String(), tt.expectedOutput)
			}
		})
	}
}
